# frozen_string_literal: true

# Converts validated intent into safe SQL with parameter binding

begin
  require 'arel'
  require 'active_record'
rescue LoadError
end

module CodeToQuery
  # rubocop:disable Metrics/ClassLength
  class Compiler
    def initialize(config)
      @config = config
    end

    def compile(intent, current_user: nil)
      intent_with_policy = apply_policy_predicates(intent, current_user)
      if use_arel?
        compile_with_arel(intent_with_policy, current_user)
      else
        compile_with_string_building(intent_with_policy, current_user)
      end
    end

    private

    def apply_policy_predicates(intent, current_user)
      return intent unless @config.policy_adapter.respond_to?(:call)

      table = intent['table']
      policy_info = safely_fetch_policy(table: table, current_user: current_user, intent: intent)
      policy_hash = extract_enforced_predicates(policy_info)
      return intent if policy_hash.empty?

      filters = Array(intent['filters']) + policy_hash.map do |column, value|
        if value.is_a?(Range) && value.begin && value.end
          {
            'column' => column.to_s,
            'op' => 'between',
            'param_start' => "policy_#{column}_start",
            'param_end' => "policy_#{column}_end"
          }
        else
          {
            'column' => column.to_s,
            'op' => '=',
            'param' => "policy_#{column}"
          }
        end
      end

      params = (intent['params'] || {}).dup
      policy_hash.each do |column, value|
        if value.is_a?(Range) && value.begin && value.end
          params["policy_#{column}_start"] = value.begin
          params["policy_#{column}_end"] = value.end
        else
          params["policy_#{column}"] = value
        end
      end

      intent.merge(
        'filters' => filters,
        'params' => params
      )
    rescue PolicyAdapterError
      raise
    rescue StandardError => e
      raise policy_failure("Policy application failed: #{e.message}") unless policy_adapter_fail_open?

      @config.logger.warn("[code_to_query] Policy application failed: #{e.message}")
      intent
    end

    def safely_fetch_policy(table:, current_user:, intent: nil)
      if intent
        @config.policy_adapter.call(current_user, table: table, intent: intent)
      else
        @config.policy_adapter.call(current_user, table: table)
      end
    rescue ArgumentError
      # Backward compatibility: adapters may accept user plus table or only user.
      begin
        @config.policy_adapter.call(current_user, table: table)
      rescue ArgumentError
        begin
          @config.policy_adapter.call(current_user)
        rescue StandardError => e
          return handle_policy_failure("Policy adapter failed: #{e.message}") if policy_adapter_fail_open?

          raise policy_failure("Policy adapter failed: #{e.message}")
        end
      rescue StandardError => e
        return handle_policy_failure("Policy adapter failed: #{e.message}") if policy_adapter_fail_open?

        raise policy_failure("Policy adapter failed: #{e.message}")
      end
    rescue StandardError => e
      return handle_policy_failure("Policy adapter failed: #{e.message}") if policy_adapter_fail_open?

      raise policy_failure("Policy adapter failed: #{e.message}")
    end

    def extract_enforced_predicates(policy_info)
      return handle_policy_failure('Policy adapter returned nil') if policy_info.nil?
      unless policy_info.is_a?(Hash)
        return handle_policy_failure("Policy adapter returned #{policy_info.class}, expected Hash")
      end

      predicates = policy_info[:enforced_predicates] || policy_info['enforced_predicates'] ||
                   policy_info[:predicates] || policy_info['predicates']
      predicates = policy_info if predicates.nil? && direct_predicate_hash?(policy_info)
      predicates ||= {}
      unless predicates.is_a?(Hash)
        return handle_policy_failure("Policy predicates must be a Hash, got #{predicates.class}")
      end

      predicates.each do |column, value|
        return handle_policy_failure('Policy predicate columns must be present') if column.to_s.strip.empty?
        next if value.is_a?(Range) && value.begin && value.end
        next unless value.nil? || value.is_a?(Hash) || value.is_a?(Array)

        return handle_policy_failure("Malformed policy predicate for #{column}")
      end

      predicates
    end

    def direct_predicate_hash?(policy_info)
      policy_info.keys.none? do |key|
        %i[enforced_predicates predicates allowed_tables allowed_columns].include?(key) ||
          %w[enforced_predicates predicates allowed_tables allowed_columns].include?(key.to_s)
      end
    end

    def policy_adapter_fail_open?
      @config.respond_to?(:policy_adapter_fail_open) && @config.policy_adapter_fail_open
    end

    def handle_policy_failure(message)
      if policy_adapter_fail_open?
        @config.logger.warn("[code_to_query] #{message}")
        return {}
      end

      raise PolicyAdapterError, message
    end

    def policy_failure(message)
      PolicyAdapterError.new(message)
    end

    def use_arel?
      defined?(Arel) && defined?(ActiveRecord::Base) && ActiveRecord::Base.connected?
    end

    def compile_with_arel(intent, current_user = nil)
      table_name = intent.fetch('table')

      table = if defined?(ActiveRecord::Base)
                begin
                  model = infer_model_for_table(table_name)
                  model ? model.arel_table : Arel::Table.new(table_name)
                rescue StandardError
                  Arel::Table.new(table_name)
                end
              else
                Arel::Table.new(table_name)
              end

      query = build_arel_select_query(intent, table)

      params_hash = normalize_params_with_model(intent)
      bind_spec = []

      if (filters = intent['filters']).present?
        where_conditions = filters.map do |filter|
          build_arel_condition(table, filter, bind_spec)
        end
        where_conditions.compact.each do |condition|
          query = query.where(condition)
        end
      end

      query = apply_arel_ordering(query, table, intent['order']) if intent['order'].present?
      query = apply_arel_aggregations(query, table, intent['aggregations']) if intent['aggregations'].present?
      query = apply_arel_grouping(query, table, intent['group_by']) if intent['group_by'].present?
      if intent['having'].present?
        query = apply_arel_having(query, table, intent['having'], bind_spec)
      end

      if (limit = determine_appropriate_limit(intent))
        query = query.take(limit)
      end

      connection = ActiveRecord::Base.connection
      visitor = connection.visitor

      sql = visitor.accept(query.ast, Arel::Collectors::SQLString.new).value

      { sql: sql, params: params_hash, bind_spec: bind_spec }
    rescue StandardError => e
      @config.logger.warn("[code_to_query] Arel compilation failed: #{e.message}")
      compile_with_string_building(intent, current_user)
    end

    def compile_with_string_building(intent, current_user = nil)
      table = intent.fetch('table')
      params_hash = normalize_params_with_model(intent)
      bind_spec = []
      placeholder_index = 1

      sql_parts = [build_string_select_clause(intent, table)]

      if (filters = intent['filters']).present?
        where_fragments = filters.map do |filter|
          fragment, placeholder_index = build_string_filter_fragment(
            filter, table, bind_spec, params_hash, placeholder_index, current_user
          )
          fragment
        end
        sql_parts << "WHERE #{where_fragments.join(' AND ')}" if where_fragments.any?
      end

      sql_parts << build_string_group_clause(intent['group_by']) if intent['group_by'].present?

      if intent['having'].present?
        having_clause, placeholder_index = build_string_having_clause(
          intent['having'], bind_spec, placeholder_index
        )
        sql_parts << having_clause
      end

      sql_parts << build_string_order_clause(intent['order']) if intent['order'].present?
      if (limit = determine_appropriate_limit(intent))
        sql_parts << build_string_limit_clause(limit)
      end

      { sql: sql_parts.join(' '), params: params_hash, bind_spec: bind_spec }
    end

    def build_arel_select_query(intent, table)
      query = table.project(*build_arel_projections(intent['columns'] || ['*'], table))
      apply_arel_distinct(query, table, intent)
    end

    def build_arel_projections(columns, table)
      parsed_functions = Array(columns).map { |c| parse_function_column(c) }.compact
      return [Arel.star] if parsed_functions.empty? && (columns == ['*'] || columns.include?('*'))
      return columns.map { |col| table[col] } if parsed_functions.empty?

      projections = parsed_functions.map { |function_spec| build_arel_function_projection(function_spec, table) }.compact
      projections.empty? ? [Arel.star] : projections
    end

    def build_arel_function_projection(function_spec, table)
      func = function_spec[:func]
      column = function_spec[:column]

      case func
      when 'count'
        node = column ? table[column].count : Arel.star.count
        node.as('count')
      when 'sum'
        table[column].sum.as('sum') if column
      when 'avg'
        table[column].average.as('avg') if column
      when 'max'
        table[column].maximum.as('max') if column
      when 'min'
        table[column].minimum.as('min') if column
      end
    end

    def apply_arel_distinct(query, table, intent)
      return query unless intent['distinct']
      return query.distinct unless intent['distinct_on']&.any?

      distinct_columns = intent['distinct_on'].map { |col| table[col] }
      query.distinct(*distinct_columns)
    end

    def apply_arel_ordering(query, table, orders)
      orders.each do |order_spec|
        column = table[order_spec['column']]
        direction = order_spec['dir']&.downcase == 'desc' ? :desc : :asc
        query = query.order(column.send(direction))
      end
      query
    end

    def apply_arel_grouping(query, table, group_columns)
      group_columns.each { |column| query = query.group(table[column]) }
      query
    end

    def apply_arel_having(query, table, having_filters, bind_spec)
      having_filters.each do |having_filter|
        agg_node = build_arel_aggregate(table, having_filter)
        next unless agg_node

        key = having_bind_key(having_filter)
        append_bind_spec(bind_spec, key: key, column: having_filter['column'])
        condition = build_arel_having_condition(agg_node, having_filter['op'], key)
        query = query.having(condition) if condition
      end
      query
    end

    def build_string_select_clause(intent, table)
      raw_columns = intent['columns'].presence || ['*']
      columns = build_string_select_projection(raw_columns)
      distinct_clause = build_string_distinct_clause(intent)

      "SELECT #{distinct_clause}#{columns} FROM #{quote_ident(table)}"
    end

    def build_string_select_projection(raw_columns)
      # Detect function columns (e.g., COUNT(*), SUM(amount)) and build proper SELECT list.
      function_specs = Array(raw_columns).map { |c| parse_function_column(c) }.compact
      return Array(raw_columns).map { |c| quote_ident(c) }.join(', ') unless function_specs.any?

      # Support single or multiple function projections.
      function_specs.map { |fn| build_string_function_projection(fn) }.join(', ')
    end

    def build_string_function_projection(function_spec)
      func = function_spec[:func]
      col = function_spec[:column]

      case func
      when 'count'
        col ? "COUNT(#{quote_ident(col)}) as count" : 'COUNT(*) as count'
      when 'sum'
        "SUM(#{quote_ident(col)}) as sum"
      when 'avg'
        "AVG(#{quote_ident(col)}) as avg"
      when 'max'
        "MAX(#{quote_ident(col)}) as max"
      when 'min'
        "MIN(#{quote_ident(col)}) as min"
      else
        quote_ident(col.to_s)
      end
    end

    def build_string_distinct_clause(intent)
      return '' unless intent['distinct']
      return 'DISTINCT ' unless intent['distinct_on']&.any?

      distinct_on_cols = intent['distinct_on'].map { |c| quote_ident(c) }.join(', ')
      "DISTINCT ON (#{distinct_on_cols}) "
    end

    def build_string_group_clause(group_columns)
      group_fragments = group_columns.map { |col| quote_ident(col) }
      "GROUP BY #{group_fragments.join(', ')}"
    end

    def build_string_having_clause(having_filters, bind_spec, placeholder_index)
      having_fragments = having_filters.map do |h|
        fragment, placeholder_index = build_string_having_fragment(h, bind_spec, placeholder_index)
        fragment
      end

      ["HAVING #{having_fragments.join(' AND ')}", placeholder_index]
    end

    def build_string_having_fragment(having_filter, bind_spec, placeholder_index)
      agg_expr = build_aggregate_expression(having_filter)
      placeholder = placeholder_for_adapter(placeholder_index)
      append_bind_spec(
        bind_spec,
        key: having_bind_key(having_filter),
        column: having_filter['column']
      )

      ["#{agg_expr} #{having_filter['op']} #{placeholder}", placeholder_index + 1]
    end

    def build_string_order_clause(orders)
      order_fragments = orders.map do |order|
        dir = order['dir'].to_s.downcase == 'desc' ? 'DESC' : 'ASC'
        "#{quote_ident(order['column'])} #{dir}"
      end

      "ORDER BY #{order_fragments.join(', ')}"
    end

    def build_string_limit_clause(limit)
      "LIMIT #{Integer(limit)}"
    end

    def build_string_filter_fragment(filter, table, bind_spec, params_hash, placeholder_index, current_user)
      col = quote_ident(filter['column'])
      case filter['op']
      when '=', '>', '<', '>=', '<=', '!=', '<>'
        build_string_comparison_fragment(col, filter, bind_spec, placeholder_index)
      when 'exists', 'not_exists'
        build_string_subquery_fragment(filter, table, bind_spec, params_hash, placeholder_index, current_user)
      when 'between'
        build_string_between_fragment(col, filter, bind_spec, placeholder_index)
      when 'in'
        key = filter_bind_key(filter)
        values = params_hash[key] || params_hash[key.to_s] || params_hash[key.to_sym]
        validate_in_clause_values!(values, filter['column'])

        placeholder = placeholder_for_adapter(placeholder_index)
        append_bind_spec(bind_spec, key: key, column: filter['column'], cast: :array)
        ["#{col} IN (#{placeholder})", placeholder_index + 1]
      when 'like', 'ilike'
        build_string_pattern_fragment(col, filter, bind_spec, placeholder_index)
      else
        raise ArgumentError, "Unsupported filter op: #{filter['op']}"
      end
    end

    def build_string_comparison_fragment(quoted_column, filter, bind_spec, placeholder_index)
      key = filter_bind_key(filter)
      placeholder = placeholder_for_adapter(placeholder_index)
      append_bind_spec(bind_spec, key: key, column: filter['column'])
      ["#{quoted_column} #{filter['op']} #{placeholder}", placeholder_index + 1]
    end

    def build_string_between_fragment(quoted_column, filter, bind_spec, placeholder_index)
      start_key, end_key = between_bind_keys(filter)

      placeholder1 = placeholder_for_adapter(placeholder_index)
      append_bind_spec(bind_spec, key: start_key, column: filter['column'])
      placeholder_index += 1

      placeholder2 = placeholder_for_adapter(placeholder_index)
      append_bind_spec(bind_spec, key: end_key, column: filter['column'])
      placeholder_index += 1

      ["#{quoted_column} BETWEEN #{placeholder1} AND #{placeholder2}", placeholder_index]
    end

    def build_string_pattern_fragment(quoted_column, filter, bind_spec, placeholder_index)
      key = filter_bind_key(filter)
      placeholder = placeholder_for_adapter(placeholder_index)
      append_bind_spec(bind_spec, key: key, column: filter['column'])
      ["#{quoted_column} #{filter['op'].upcase} #{placeholder}", placeholder_index + 1]
    end

    def build_string_subquery_fragment(filter, table, bind_spec, params_hash, placeholder_index, current_user)
      related_table = filter['related_table']
      fk_column = filter['fk_column']
      base_column = filter['base_column'] || 'id'
      related_filters = Array(filter['related_filters'])
      op = filter['op']

      raise ArgumentError, "#{op} requires related_table and fk_column" if related_table.nil? || fk_column.nil?

      rt = quote_ident(related_table)
      fk_col = quote_ident(fk_column)
      base_col = quote_ident(base_column)

      sub_where = ["#{rt}.#{fk_col} = #{quote_ident(table)}.#{base_col}"]
      sub_where, placeholder_index = apply_policy_in_subquery(
        sub_where, bind_spec, params_hash, related_table, placeholder_index, current_user
      )

      related_filters.each do |related_filter|
        fragment, placeholder_index = build_string_subquery_filter_fragment(
          rt, related_filter, bind_spec, params_hash, placeholder_index
        )
        sub_where << fragment
      end

      keyword = op == 'not_exists' ? 'NOT EXISTS' : 'EXISTS'
      ["#{keyword} (SELECT 1 FROM #{rt} WHERE #{sub_where.join(' AND ')})", placeholder_index]
    end

    def build_string_subquery_filter_fragment(quoted_related_table, filter, bind_spec, params_hash, placeholder_index)
      quoted_column = "#{quoted_related_table}.#{quote_ident(filter['column'])}"
      case filter['op']
      when '=', '>', '<', '>=', '<=', '!=', '<>'
        build_string_comparison_fragment(quoted_column, filter, bind_spec, placeholder_index)
      when 'between'
        build_string_between_fragment(quoted_column, filter, bind_spec, placeholder_index)
      when 'in'
        key = filter_bind_key(filter)
        values = params_hash[key] || params_hash[key.to_s] || params_hash[key.to_sym]
        validate_in_clause_values!(values, filter['column'])
        placeholder = placeholder_for_adapter(placeholder_index)
        append_bind_spec(bind_spec, key: key, column: filter['column'], cast: :array)
        ["#{quoted_column} IN (#{placeholder})", placeholder_index + 1]
      when 'like', 'ilike'
        build_string_pattern_fragment(quoted_column, filter, bind_spec, placeholder_index)
      else
        raise ArgumentError, "Unsupported filter op in subquery: #{filter['op']}"
      end
    end

    def apply_policy_in_subquery(sub_where, bind_spec, params_hash, related_table, placeholder_index, current_user)
      return [sub_where, placeholder_index] unless @config.policy_adapter.respond_to?(:call)

      info = safely_fetch_policy(table: related_table, current_user: current_user)
      predicates = extract_enforced_predicates(info)
      return [sub_where, placeholder_index] unless predicates.is_a?(Hash) && predicates.any?

      predicates.each do |column, value|
        rcol = "#{quote_ident(related_table)}.#{quote_ident(column)}"
        policy_key_prefix = subquery_policy_key_prefix(related_table, column, placeholder_index)
        if value.is_a?(Range) && value.begin && value.end
          start_key = "#{policy_key_prefix}_start"
          end_key = "#{policy_key_prefix}_end"
          params_hash[start_key] = value.begin
          params_hash[end_key] = value.end
          p1 = placeholder_for_adapter(placeholder_index)
          append_bind_spec(bind_spec, key: start_key, column: column)
          placeholder_index += 1
          p2 = placeholder_for_adapter(placeholder_index)
          append_bind_spec(bind_spec, key: end_key, column: column)
          placeholder_index += 1
          sub_where << "#{rcol} BETWEEN #{p1} AND #{p2}"
        else
          key = policy_key_prefix
          params_hash[key] = value
          p = placeholder_for_adapter(placeholder_index)
          append_bind_spec(bind_spec, key: key, column: column)
          placeholder_index += 1
          sub_where << "#{rcol} = #{p}"
        end
      end

      [sub_where, placeholder_index]
    rescue PolicyAdapterError
      raise
    rescue StandardError => e
      raise policy_failure("Policy application failed in subquery: #{e.message}") unless policy_adapter_fail_open?

      [sub_where, placeholder_index]
    end

    def subquery_policy_key_prefix(table, column, placeholder_index)
      safe_table = policy_key_fragment(table)
      safe_column = policy_key_fragment(column)

      "policy_subquery_#{placeholder_index}_#{safe_table}_#{safe_column}"
    end

    def policy_key_fragment(value)
      value.to_s.gsub(/[^a-zA-Z0-9_]/, '_')
    end

    def build_arel_condition(table, filter, bind_spec)
      column = table[filter['column']]
      operator = filter['op']

      case operator
      when '='
        key = filter_bind_key(filter)
        append_bind_spec(bind_spec, key: key, column: filter['column'])
        column.eq(Arel::Nodes::BindParam.new(key))
      when '!=', '<>'
        key = filter_bind_key(filter)
        append_bind_spec(bind_spec, key: key, column: filter['column'])
        column.not_eq(Arel::Nodes::BindParam.new(key))
      when 'exists'
        # Force fallback to string builder for complex correlated subqueries
        raise StandardError, 'exists Arel compilation is not implemented; falling back to string builder'
      when 'not_exists'
        # Force fallback to string builder for complex correlated subqueries
        raise StandardError, 'not_exists Arel compilation is not implemented; falling back to string builder'
      when '>'
        key = filter_bind_key(filter)
        append_bind_spec(bind_spec, key: key, column: filter['column'])
        column.gt(Arel::Nodes::BindParam.new(key))
      when '>='
        key = filter_bind_key(filter)
        append_bind_spec(bind_spec, key: key, column: filter['column'])
        column.gteq(Arel::Nodes::BindParam.new(key))
      when '<'
        key = filter_bind_key(filter)
        append_bind_spec(bind_spec, key: key, column: filter['column'])
        column.lt(Arel::Nodes::BindParam.new(key))
      when '<='
        key = filter_bind_key(filter)
        append_bind_spec(bind_spec, key: key, column: filter['column'])
        column.lteq(Arel::Nodes::BindParam.new(key))
      when 'between'
        start_key, end_key = between_bind_keys(filter)
        append_bind_spec(bind_spec, key: start_key, column: filter['column'])
        append_bind_spec(bind_spec, key: end_key, column: filter['column'])

        start_param = Arel::Nodes::BindParam.new(start_key)
        end_param = Arel::Nodes::BindParam.new(end_key)
        column.between(start_param..end_param)
      when 'in'
        key = filter_bind_key(filter)
        append_bind_spec(bind_spec, key: key, column: filter['column'], cast: :array)
        column.in(Arel::Nodes::BindParam.new(key))
      when 'like'
        key = filter_bind_key(filter)
        append_bind_spec(bind_spec, key: key, column: filter['column'])
        column.matches(Arel::Nodes::BindParam.new(key))
      when 'ilike'
        key = filter_bind_key(filter)
        append_bind_spec(bind_spec, key: key, column: filter['column'])
        # ilike is PostgreSQL-specific
        Arel::Nodes::Matches.new(column, Arel::Nodes::BindParam.new(key), nil, true)
      else
        warn "[code_to_query] Unsupported Arel operator: #{operator}"
        nil
      end
    end

    def filter_bind_key(filter)
      filter['param'] || filter['column']
    end

    def between_bind_keys(filter)
      [filter['param_start'] || 'start', filter['param_end'] || 'end']
    end

    def having_bind_key(having_filter)
      having_filter['param'] || "having_#{having_filter['column']}"
    end

    def append_bind_spec(bind_spec, key:, column:, cast: nil)
      bind_spec << { key: key, column: column, cast: cast }
    end

    def placeholder_for_adapter(index)
      case @config.adapter
      when :postgres, :postgresql
        "$#{index}"
      when :mysql
        '?'
      when :sqlite
        '?'
      else
        '?' # Safe default
      end
    end

    def quote_ident(name)
      return name if name == '*'

      name = name.to_s

      case @config.adapter
      when :postgres, :postgresql
        %("#{name.gsub('"', '""')}")
      when :mysql
        "`#{name.gsub('`', '``')}`"
      when :sqlite
        %("#{name.gsub('"', '""')}")
      else
        %("#{name.gsub('"', '""')}")
      end
    end

    def apply_arel_aggregations(query, table, aggregations)
      aggregations.each do |agg|
        case agg['type']
        when 'count'
          column = agg['column'] ? table[agg['column']] : Arel.star
          query = query.project(column.count.as('count'))
        when 'sum'
          return query unless agg['column']

          query = query.project(table[agg['column']].sum.as('sum'))
        when 'avg'
          return query unless agg['column']

          query = query.project(table[agg['column']].average.as('avg'))
        when 'max'
          return query unless agg['column']

          query = query.project(table[agg['column']].maximum.as('max'))
        when 'min'
          return query unless agg['column']

          query = query.project(table[agg['column']].minimum.as('min'))
        end
      end
      query
    end

    def determine_appropriate_limit(intent)
      # Explicit limit always takes precedence
      return intent['limit'] if intent['limit']

      # Determine query type and apply appropriate limit
      if has_count_aggregation?(intent)
        @config.count_limit
      elsif has_non_count_aggregations?(intent)
        @config.aggregation_limit
      elsif has_exists_checks?(intent)
        @config.exists_limit
      elsif intent['distinct']
        @config.distinct_limit
      else
        @config.default_limit
      end
    end

    def has_count_aggregation?(intent)
      Array(intent['aggregations']).any? { |a| a['type'].to_s.downcase == 'count' } ||
        Array(intent['columns']).any? { |c| c.to_s.match?(/\bcount\s*\(/i) }
    end

    def has_non_count_aggregations?(intent)
      Array(intent['aggregations']).any? { |a| %w[sum avg max min].include?(a['type'].to_s.downcase) } ||
        Array(intent['columns']).any? { |c| c.to_s.match?(/\b(sum|avg|max|min)\s*\(/i) }
    end

    def has_exists_checks?(intent)
      intent['filters']&.any? { |filter| %w[exists not_exists].include?(filter['op']) }
    end

    def parse_function_column(expr)
      return nil unless expr

      s = expr.to_s.strip
      return nil unless s.include?('(') && s.end_with?(')')

      if (m = s.match(/\A\s*(count|sum|avg|max|min)\s*\(\s*(\*|[a-zA-Z0-9_.]+)\s*\)\s*\z/i))
        func = m[1].downcase
        col = m[2] == '*' ? nil : m[2]
        { func: func, column: col }
      end
    end

    def build_aggregate_expression(having_spec)
      func = having_spec['function'].to_s.upcase
      col = having_spec['column']

      case func
      when 'COUNT'
        col ? "COUNT(#{quote_ident(col)})" : 'COUNT(*)'
      when 'SUM'
        "SUM(#{quote_ident(col)})"
      when 'AVG'
        "AVG(#{quote_ident(col)})"
      when 'MAX'
        "MAX(#{quote_ident(col)})"
      when 'MIN'
        "MIN(#{quote_ident(col)})"
      else
        'COUNT(*)'
      end
    end

    def validate_in_clause_values!(values, column)
      return unless values.is_a?(Array) && values.empty?

      raise ArgumentError, "IN clause requires non-empty array for column '#{column}'"
    end

    def build_arel_aggregate(table, having_spec)
      func = having_spec['function'].to_s.downcase
      col = having_spec['column']

      case func
      when 'count'
        col ? table[col].count : Arel.star.count
      when 'sum'
        return nil unless col

        table[col].sum
      when 'avg'
        return nil unless col

        table[col].average
      when 'max'
        return nil unless col

        table[col].maximum
      when 'min'
        return nil unless col

        table[col].minimum
      else
        Arel.star.count
      end
    end

    def build_arel_having_condition(agg_node, operator, key)
      bind_param = Arel::Nodes::BindParam.new(key)

      case operator
      when '='
        agg_node.eq(bind_param)
      when '!='
        agg_node.not_eq(bind_param)
      when '>'
        agg_node.gt(bind_param)
      when '>='
        agg_node.gteq(bind_param)
      when '<'
        agg_node.lt(bind_param)
      when '<='
        agg_node.lteq(bind_param)
      end
    end

    def normalize_params_with_model(intent)
      params = (intent['params'] || {}).dup
      return params unless defined?(ActiveRecord::Base)

      table_name = intent['table']
      model = infer_model_for_table(table_name)
      return params unless model

      enum_map = model.respond_to?(:defined_enums) ? model.defined_enums : {}
      Array(intent['filters']).each do |f|
        col = f['column']
        key = f['param'] || col
        next unless key

        raw = params[key.to_s] || params[key.to_sym]
        next if raw.nil?

        # Map Rails enum string to integer
        mapping = enum_map[col] || enum_map[col.to_s]
        if mapping.is_a?(Hash) && raw.is_a?(String)
          val = mapping[raw] || mapping[raw.downcase]
          params[key.to_s] = Integer(val) if val
        end
      end

      params
    rescue StandardError
      intent['params'] || {}
    end

    def infer_model_for_table(table_name)
      return nil unless defined?(ActiveRecord::Base)
      return nil unless table_name

      possible_class_names = [
        table_name.singularize.camelize,
        table_name.camelize,
        table_name.singularize.camelize.gsub(/s$/, '')
      ]

      possible_class_names.each do |class_name|
        model = class_name.constantize
        return model if model < ActiveRecord::Base && model.table_name == table_name
      rescue NameError
        next
      end

      nil
    end
  end
  # rubocop:enable Metrics/ClassLength
end
