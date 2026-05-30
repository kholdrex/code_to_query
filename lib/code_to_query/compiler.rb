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

      columns = intent['columns'] || ['*']
      parsed_functions = Array(columns).map { |c| parse_function_column(c) }.compact
      if parsed_functions.any?
        projections = parsed_functions.map do |fn|
          func = fn[:func]
          col  = fn[:column]
          case func
          when 'count'
            node = col ? table[col].count : Arel.star.count
            node.as('count')
          when 'sum'
            next unless col

            table[col].sum.as('sum')
          when 'avg'
            next unless col

            table[col].average.as('avg')
          when 'max'
            next unless col

            table[col].maximum.as('max')
          when 'min'
            next unless col

            table[col].minimum.as('min')
          end
        end.compact
        projections = [Arel.star] if projections.empty?
        query = table.project(*projections)
      elsif columns == ['*'] || columns.include?('*')
        query = table.project(Arel.star)
      else
        projections = columns.map { |col| table[col] }
        query = table.project(*projections)
      end

      if intent['distinct']
        if intent['distinct_on']&.any?
          # PostgreSQL DISTINCT ON
          distinct_columns = intent['distinct_on'].map { |col| table[col] }
          query = query.distinct(*distinct_columns)
        else
          query = query.distinct
        end
      end

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

      if (orders = intent['order']).present?
        orders.each do |order_spec|
          column = table[order_spec['column']]
          direction = order_spec['dir']&.downcase == 'desc' ? :desc : :asc
          query = query.order(column.send(direction))
        end
      end

      if (aggregations = intent['aggregations']).present?
        query = apply_arel_aggregations(query, table, aggregations)
      end

      if (group_columns = intent['group_by']).present?
        group_columns.each do |col|
          query = query.group(table[col])
        end
      end

      if (having_filters = intent['having']).present?
        having_filters.each do |h|
          agg_node = build_arel_aggregate(table, h)
          next unless agg_node

          key = h['param'] || "having_#{h['column']}"
          bind_spec << { key: key, column: h['column'], cast: nil }
          condition = build_arel_having_condition(agg_node, h['op'], key)
          query = query.having(condition) if condition
        end
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

    # rubocop:disable Metrics/AbcSize, Metrics/MethodLength, Metrics/BlockLength, Metrics/CyclomaticComplexity
    # NOTE: This method is intentionally monolithic for clarity and to avoid regressions in SQL assembly.
    # TODO: Extract EXISTS/NOT EXISTS handling and simple predicate building into small helpers.
    def compile_with_string_building(intent, current_user = nil)
      table = intent.fetch('table')
      # Detect function columns (e.g., COUNT(*), SUM(amount)) and build proper SELECT list
      raw_columns = intent['columns'].presence || ['*']
      function_specs = Array(raw_columns).map { |c| parse_function_column(c) }.compact
      columns = if function_specs.any?
                  # Support single or multiple function projections
                  function_specs.map do |fn|
                    func = fn[:func]
                    col  = fn[:column]
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
                  end.join(', ')
                else
                  Array(raw_columns).map { |c| quote_ident(c) }.join(', ')
                end

      # Handle DISTINCT
      distinct_clause = ''
      if intent['distinct']
        if intent['distinct_on']&.any?
          # PostgreSQL DISTINCT ON
          distinct_on_cols = intent['distinct_on'].map { |c| quote_ident(c) }.join(', ')
          distinct_clause = "DISTINCT ON (#{distinct_on_cols}) "
        else
          distinct_clause = 'DISTINCT '
        end
      end

      sql_parts = []
      sql_parts << "SELECT #{distinct_clause}#{columns} FROM #{quote_ident(table)}"

      params_hash = normalize_params_with_model(intent)
      bind_spec = []
      placeholder_index = 1

      if (filters = intent['filters']).present?
        where_fragments = filters.map do |f|
          col = quote_ident(f['column'])
          case f['op']
          when '=', '>', '<', '>=', '<=', '!=', '<>'
            key = f['param'] || f['column']
            placeholder = placeholder_for_adapter(placeholder_index)
            bind_spec << { key: key, column: f['column'], cast: nil }
            fragment = "#{col} #{f['op']} #{placeholder}"
            placeholder_index += 1
            fragment
          when 'exists'
            related_table = f['related_table']
            fk_column = f['fk_column']
            base_column = f['base_column'] || 'id'
            related_filters = Array(f['related_filters'])

            raise ArgumentError, 'exists requires related_table and fk_column' if related_table.nil? || fk_column.nil?

            rt = quote_ident(related_table)
            fk_col = quote_ident(fk_column)
            base_col = quote_ident(base_column)

            sub_where = []
            sub_where << "#{rt}.#{fk_col} = #{quote_ident(table)}.#{base_col}"

            # Inject policy predicates for related table if available
            sub_where, placeholder_index = apply_policy_in_subquery(
              sub_where, bind_spec, params_hash, related_table, placeholder_index, current_user
            )

            related_filters.each do |rf|
              rcol = "#{rt}.#{quote_ident(rf['column'])}"
              case rf['op']
              when '=', '>', '<', '>=', '<=', '!=', '<>'
                key = rf['param'] || rf['column']
                placeholder = placeholder_for_adapter(placeholder_index)
                bind_spec << ({ key: key, column: rf['column'], cast: nil })
                sub_where << "#{rcol} #{rf['op']} #{placeholder}"
                placeholder_index += 1
              when 'between'
                start_key = rf['param_start'] || 'start'
                end_key = rf['param_end'] || 'end'
                placeholder1 = placeholder_for_adapter(placeholder_index)
                bind_spec << ({ key: start_key, column: rf['column'], cast: nil })
                placeholder_index += 1
                placeholder2 = placeholder_for_adapter(placeholder_index)
                bind_spec << ({ key: end_key, column: rf['column'], cast: nil })
                placeholder_index += 1
                sub_where << "#{rcol} BETWEEN #{placeholder1} AND #{placeholder2}"
              when 'in'
                key = rf['param'] || rf['column']
                values = params_hash[key] || params_hash[key.to_s] || params_hash[key.to_sym]
                validate_in_clause_values!(values, rf['column'])
                placeholder = placeholder_for_adapter(placeholder_index)
                bind_spec << ({ key: key, column: rf['column'], cast: :array })
                placeholder_index += 1
                sub_where << "#{rcol} IN (#{placeholder})"
              when 'like', 'ilike'
                key = rf['param'] || rf['column']
                placeholder = placeholder_for_adapter(placeholder_index)
                bind_spec << ({ key: key, column: rf['column'], cast: nil })
                placeholder_index += 1
                sub_where << "#{rcol} #{rf['op'].upcase} #{placeholder}"
              else
                raise ArgumentError, "Unsupported filter op in subquery: #{rf['op']}"
              end
            end

            "EXISTS (SELECT 1 FROM #{rt} WHERE #{sub_where.join(' AND ')})"
          when 'not_exists'
            # Correlated NOT EXISTS subquery against a related table
            related_table = f['related_table']
            fk_column = f['fk_column']
            base_column = f['base_column'] || 'id'
            related_filters = Array(f['related_filters'])

            raise ArgumentError, 'not_exists requires related_table and fk_column' if related_table.nil? || fk_column.nil?

            rt = quote_ident(related_table)
            fk_col = quote_ident(fk_column)
            base_col = quote_ident(base_column)

            sub_where = []
            # Correlation predicate
            sub_where << "#{rt}.#{fk_col} = #{quote_ident(table)}.#{base_col}"

            # Inject policy predicates for related table if available
            sub_where, placeholder_index = apply_policy_in_subquery(
              sub_where, bind_spec, params_hash, related_table, placeholder_index, current_user
            )

            # Additional predicates within the subquery
            related_filters.each do |rf|
              rcol = "#{rt}.#{quote_ident(rf['column'])}"
              case rf['op']
              when '=', '>', '<', '>=', '<=', '!=', '<>'
                key = rf['param'] || rf['column']
                placeholder = placeholder_for_adapter(placeholder_index)
                bind_spec << { key: key, column: rf['column'], cast: nil }
                sub_where << "#{rcol} #{rf['op']} #{placeholder}"
                placeholder_index += 1
              when 'between'
                start_key = rf['param_start'] || 'start'
                end_key = rf['param_end'] || 'end'

                placeholder1 = placeholder_for_adapter(placeholder_index)
                bind_spec << { key: start_key, column: rf['column'], cast: nil }
                placeholder_index += 1

                placeholder2 = placeholder_for_adapter(placeholder_index)
                bind_spec << { key: end_key, column: rf['column'], cast: nil }
                placeholder_index += 1

                sub_where << "#{rcol} BETWEEN #{placeholder1} AND #{placeholder2}"
              when 'in'
                key = rf['param'] || rf['column']
                values = params_hash[key] || params_hash[key.to_s] || params_hash[key.to_sym]
                validate_in_clause_values!(values, rf['column'])
                placeholder = placeholder_for_adapter(placeholder_index)
                bind_spec << { key: key, column: rf['column'], cast: :array }
                placeholder_index += 1
                sub_where << "#{rcol} IN (#{placeholder})"
              when 'like', 'ilike'
                key = rf['param'] || rf['column']
                placeholder = placeholder_for_adapter(placeholder_index)
                bind_spec << { key: key, column: rf['column'], cast: nil }
                placeholder_index += 1
                sub_where << "#{rcol} #{rf['op'].upcase} #{placeholder}"
              else
                raise ArgumentError, "Unsupported filter op in subquery: #{rf['op']}"
              end
            end

            "NOT EXISTS (SELECT 1 FROM #{rt} WHERE #{sub_where.join(' AND ')})"
          when 'between'
            start_key = f['param_start'] || 'start'
            end_key = f['param_end'] || 'end'

            placeholder1 = placeholder_for_adapter(placeholder_index)
            bind_spec << { key: start_key, column: f['column'], cast: nil }
            placeholder_index += 1

            placeholder2 = placeholder_for_adapter(placeholder_index)
            bind_spec << { key: end_key, column: f['column'], cast: nil }
            placeholder_index += 1

            "#{col} BETWEEN #{placeholder1} AND #{placeholder2}"
          when 'in'
            key = f['param'] || f['column']
            values = params_hash[key] || params_hash[key.to_s] || params_hash[key.to_sym]
            if values.is_a?(Array) && values.empty?
              raise ArgumentError, "IN clause requires non-empty array for column '#{f['column']}'"
            end

            placeholder = placeholder_for_adapter(placeholder_index)
            bind_spec << { key: key, column: f['column'], cast: :array }
            placeholder_index += 1
            "#{col} IN (#{placeholder})"
          when 'like', 'ilike'
            key = f['param'] || f['column']
            placeholder = placeholder_for_adapter(placeholder_index)
            bind_spec << { key: key, column: f['column'], cast: nil }
            placeholder_index += 1
            "#{col} #{f['op'].upcase} #{placeholder}"
          else
            raise ArgumentError, "Unsupported filter op: #{f['op']}"
          end
        end
        sql_parts << "WHERE #{where_fragments.join(' AND ')}" if where_fragments.any?
      end

      if (group_columns = intent['group_by']).present?
        group_fragments = group_columns.map { |col| quote_ident(col) }
        sql_parts << "GROUP BY #{group_fragments.join(', ')}"
      end

      if (having_filters = intent['having']).present?
        having_fragments = having_filters.map do |h|
          agg_expr = build_aggregate_expression(h)
          placeholder = placeholder_for_adapter(placeholder_index)
          bind_spec << { key: h['param'] || "having_#{h['column']}", column: h['column'], cast: nil }
          placeholder_index += 1
          "#{agg_expr} #{h['op']} #{placeholder}"
        end
        sql_parts << "HAVING #{having_fragments.join(' AND ')}"
      end

      if (orders = intent['order']).present?
        order_fragments = orders.map do |o|
          dir = o['dir'].to_s.downcase == 'desc' ? 'DESC' : 'ASC'
          "#{quote_ident(o['column'])} #{dir}"
        end
        sql_parts << "ORDER BY #{order_fragments.join(', ')}"
      end

      if (limit = determine_appropriate_limit(intent))
        sql_parts << "LIMIT #{Integer(limit)}"
      end

      { sql: sql_parts.join(' '), params: params_hash, bind_spec: bind_spec }
    end
    # rubocop:enable Metrics/AbcSize, Metrics/MethodLength, Metrics/BlockLength, Metrics/CyclomaticComplexity

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
          bind_spec << { key: start_key, column: column, cast: nil }
          placeholder_index += 1
          p2 = placeholder_for_adapter(placeholder_index)
          bind_spec << { key: end_key, column: column, cast: nil }
          placeholder_index += 1
          sub_where << "#{rcol} BETWEEN #{p1} AND #{p2}"
        else
          key = policy_key_prefix
          params_hash[key] = value
          p = placeholder_for_adapter(placeholder_index)
          bind_spec << { key: key, column: column, cast: nil }
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
        key = filter['param'] || filter['column']
        bind_spec << { key: key, column: filter['column'], cast: nil }
        column.eq(Arel::Nodes::BindParam.new(key))
      when '!=', '<>'
        key = filter['param'] || filter['column']
        bind_spec << { key: key, column: filter['column'], cast: nil }
        column.not_eq(Arel::Nodes::BindParam.new(key))
      when 'exists'
        # Force fallback to string builder for complex correlated subqueries
        raise StandardError, 'exists Arel compilation is not implemented; falling back to string builder'
      when 'not_exists'
        # Force fallback to string builder for complex correlated subqueries
        raise StandardError, 'not_exists Arel compilation is not implemented; falling back to string builder'
      when '>'
        key = filter['param'] || filter['column']
        bind_spec << { key: key, column: filter['column'], cast: nil }
        column.gt(Arel::Nodes::BindParam.new(key))
      when '>='
        key = filter['param'] || filter['column']
        bind_spec << { key: key, column: filter['column'], cast: nil }
        column.gteq(Arel::Nodes::BindParam.new(key))
      when '<'
        key = filter['param'] || filter['column']
        bind_spec << { key: key, column: filter['column'], cast: nil }
        column.lt(Arel::Nodes::BindParam.new(key))
      when '<='
        key = filter['param'] || filter['column']
        bind_spec << { key: key, column: filter['column'], cast: nil }
        column.lteq(Arel::Nodes::BindParam.new(key))
      when 'between'
        start_key = filter['param_start'] || 'start'
        end_key = filter['param_end'] || 'end'
        bind_spec << { key: start_key, column: filter['column'], cast: nil }
        bind_spec << { key: end_key, column: filter['column'], cast: nil }

        start_param = Arel::Nodes::BindParam.new(start_key)
        end_param = Arel::Nodes::BindParam.new(end_key)
        column.between(start_param..end_param)
      when 'in'
        key = filter['param'] || filter['column']
        bind_spec << { key: key, column: filter['column'], cast: :array }
        column.in(Arel::Nodes::BindParam.new(key))
      when 'like'
        key = filter['param'] || filter['column']
        bind_spec << { key: key, column: filter['column'], cast: nil }
        column.matches(Arel::Nodes::BindParam.new(key))
      when 'ilike'
        key = filter['param'] || filter['column']
        bind_spec << { key: key, column: filter['column'], cast: nil }
        # ilike is PostgreSQL-specific
        Arel::Nodes::Matches.new(column, Arel::Nodes::BindParam.new(key), nil, true)
      else
        warn "[code_to_query] Unsupported Arel operator: #{operator}"
        nil
      end
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
