# frozen_string_literal: true

begin
  require 'active_record'
rescue LoadError
end

require_relative 'query/sql_scanning'

module CodeToQuery
  class Query
    attr_reader :sql, :params, :intent, :metrics

    def initialize(sql:, params:, bind_spec:, intent:, allow_tables:, config:)
      @sql = sql
      @params = normalize_params_with_between_defaults(params || {}, intent['filters'])
      @bind_spec = bind_spec || []
      @intent = intent || {}
      @allow_tables = allow_tables
      @config = config
      @safety_checked = false
      @safety_result = nil
      @metrics = extract_metrics_from_intent(@intent)
    end

    def binds
      return [] unless defined?(ActiveRecord::Base)

      connection = if @config.readonly_role && ActiveRecord.respond_to?(:connected_to)
                     ActiveRecord::Base.connected_to(role: @config.readonly_role) do
                       ActiveRecord::Base.connection
                     end
                   else
                     ActiveRecord::Base.connection
                   end

      @bind_spec.map do |bind_info|
        key = bind_info[:key]
        column_name = bind_info[:column]

        # Get parameter value (check both string and symbol keys)
        value = param_value_for_key(@params, key)

        # Determine the correct ActiveRecord type
        type = infer_column_type(connection, @intent['table'], column_name, bind_info[:cast], key)

        ActiveRecord::Relation::QueryAttribute.new(column_name.to_s, value, type)
      end
    rescue StandardError => e
      @config.logger.warn("[code_to_query] Failed to build binds: #{e.message}")
      []
    end

    def safe?
      return @safety_result if @safety_checked

      @safety_checked = true
      @safety_result = perform_safety_checks
    end

    def explain
      CodeToQuery::Instrumentation.instrument(:explain, telemetry_payload) do
        next 'EXPLAIN unavailable (ActiveRecord not loaded)' unless defined?(ActiveRecord::Base)

        explain_sql = case @config.adapter
                      when :postgres, :postgresql
                        "EXPLAIN (ANALYZE false, VERBOSE false, BUFFERS false) #{@sql}"
                      when :mysql
                        "EXPLAIN #{@sql}"
                      when :sqlite
                        "EXPLAIN QUERY PLAN #{@sql}"
                      else
                        "EXPLAIN #{@sql}"
                      end

        result = if @config.readonly_role && ActiveRecord.respond_to?(:connected_to)
                   ActiveRecord::Base.connected_to(role: @config.readonly_role) do
                     ActiveRecord::Base.connection.execute(explain_sql)
                   end
                 else
                   ActiveRecord::Base.connection.execute(explain_sql)
                 end

        format_explain_result(result)
      end
    rescue StandardError => e
      "EXPLAIN failed: #{e.message}"
    end

    def to_relation
      return nil unless defined?(ActiveRecord::Base)
      return nil unless relationable?

      table_name = @intent['table']
      model = infer_model_for_table(table_name)
      return nil unless model

      scope = model.all

      # Apply WHERE conditions
      Array(@intent['filters']).each do |filter|
        scope = apply_filter_to_scope(scope, filter)
      end

      # Apply ORDER BY
      Array(@intent['order']).each do |order_spec|
        column = order_spec['column']
        direction = order_spec['dir']&.downcase == 'asc' ? :asc : :desc
        scope = scope.order(column => direction)
      end

      # Apply LIMIT (intelligent based on query type)
      limit = determine_appropriate_limit
      scope.limit(limit) if limit
    rescue StandardError => e
      @config.logger.warn("[code_to_query] Failed to build relation: #{e.message}")
      nil
    end

    def to_active_record
      to_relation
    end

    def relationable?
      return false unless defined?(ActiveRecord::Base)
      return false unless @intent['type'] == 'select'
      return false if compiler_only_subquery_policy_filters?

      !!infer_model_for_table(@intent['table'])
    end

    def to_relation!
      rel = to_relation
      return rel if rel

      raise CodeToQuery::NotRelationConvertibleError, 'Query cannot be expressed as ActiveRecord::Relation'
    end

    def preview
      {
        sql: @sql,
        params: preview_params,
        applied_policies: applied_policy_keys,
        estimated_cost: nil,
        would_run?: preview_would_run?
      }
    end

    def run
      CodeToQuery::Instrumentation.instrument(:run, telemetry_payload) do
        Runner.new(@config).run(sql: @sql, binds: binds)
      end
    end

    private

    def telemetry_payload
      {
        table: @intent['table'],
        query_type: @intent['type'],
        query_shape: query_shape,
        limit: @intent['limit'],
        row_limit: @intent['limit'],
        filter_count: Array(@intent['filters']).length,
        join_count: Array(@intent['joins']).length,
        policy_applied: policy_applied?,
        bind_count: Array(@bind_spec).length
      }
    end

    def normalize_params_with_between_defaults(params, filters)
      normalized = params.dup
      normalize_between_filter_params(Array(filters), normalized)
      normalized
    end

    def normalize_between_filter_params(filters, normalized_params)
      Array(filters).each do |filter|
        if filter['op'].to_s == 'between'
          hydrate_between_param_defaults(filter, normalized_params)
        end

        normalize_between_filter_params(Array(filter['related_filters']), normalized_params)
      end
    end

    def hydrate_between_param_defaults(filter, normalized_params)
      start_key, end_key = between_filter_keys(filter)
      return if between_param_key_present?(normalized_params, start_key) &&
                between_param_key_present?(normalized_params, end_key)

      legacy_start = param_value_for_key(normalized_params, 'start')
      legacy_end = param_value_for_key(normalized_params, 'end')

      if !filter['param_start'] && between_param_key_present?(normalized_params, 'start') && !between_param_key_present?(normalized_params, start_key)
        normalized_params[start_key] = legacy_start
      end

      return unless !filter['param_end'] && between_param_key_present?(normalized_params, 'end') && !between_param_key_present?(normalized_params, end_key)

      normalized_params[end_key] = legacy_end
    end

    def between_filter_keys(filter)
      [
        filter['param_start'] || default_between_filter_key(filter['column'], 'start'),
        filter['param_end'] || default_between_filter_key(filter['column'], 'end')
      ]
    end

    def default_between_filter_key(column_name, bound_side)
      return bound_side if column_name.to_s.strip == ''

      "#{column_name.to_s.strip.gsub(/[^a-zA-Z0-9_]/, '_')}_#{bound_side}"
    end

    def between_param_key_present?(params, key)
      params.key?(key) || params.key?(key.to_sym) || params.key?(key.to_s)
    end

    def param_value_for_key(params, key)
      return params[key] if params.key?(key)
      return params[key.to_s] if params.key?(key.to_s)

      params[key.to_sym]
    end

    def query_shape
      [@intent['type'], @intent['table']].compact.join(':')
    end

    def preview_params
      @params.respond_to?(:deep_dup) ? @params.deep_dup : @params.dup
    end

    def applied_policy_keys
      # Policy predicates compiled by CodeToQuery must surface as policy-prefixed
      # bind keys. Surface only those bound keys so preview callers can audit
      # policy application without trusting raw params or exposing bind values.
      Array(@bind_spec).filter_map do |bind|
        key = bind[:key]
        key.to_s if key.to_s.start_with?('policy_')
      end.uniq
    end

    def policy_applied?
      applied_policy_keys.any?
    end

    def preview_would_run?
      lint_sql!
      true
    rescue SecurityError
      false
    end

    def compiler_only_subquery_policy_filters?
      Array(@intent['filters']).any? do |filter|
        next false unless %w[exists not_exists].include?(filter['op'].to_s)

        related_policy_keys = Array(@intent['__policy_expected_keys']).grep(/\Apolicy_subquery_/)
        related_policy_keys.any?
      end
    end

    def extract_metrics_from_intent(intent)
      data = intent.is_a?(Hash) ? intent['_metrics'] : nil
      return {} unless data.is_a?(Hash)

      {
        prompt_tokens: data[:prompt_tokens] || data['prompt_tokens'],
        completion_tokens: data[:completion_tokens] || data['completion_tokens'],
        total_tokens: data[:total_tokens] || data['total_tokens'],
        elapsed_s: data[:elapsed_s] || data['elapsed_s']
      }.compact
    end

    def perform_safety_checks
      # Basic SQL structure checks
      lint_sql!

      # EXPLAIN-based performance checks
      return false if @config.enable_explain_gate && !Guardrails::ExplainGate.new(@config).allowed?(
        @sql,
        query_shape: query_shape,
        table: @intent['table'],
        query_type: @intent['type'],
        row_limit: @intent['limit'],
        policy_applied: policy_applied?
      )

      # Policy enforcement
      return false if @config.policy_adapter && !check_policy_compliance

      true
    rescue SecurityError => e
      @config.logger.warn("[code_to_query] Security check failed: #{e.message}")
      false
    rescue StandardError => e
      @config.logger.warn("[code_to_query] Safety check failed: #{e.message}")
      false
    end

    def check_policy_compliance
      # Predicates are injected at compile time with proper binds.
      # Verify via bind_spec or params keys rather than scanning SQL text.
      return true unless @config.policy_adapter

      expected_keys = expected_policy_keys
      return true if expected_keys.empty?

      bind_keys = Array(@bind_spec).filter_map { |bind| bind[:key]&.to_s }
      param_keys = @params.keys.map(&:to_s)

      expected_keys.all? { |key| bind_keys.include?(key) && param_keys.include?(key) }
    end

    def policy_predicates_expected?
      expected_policy_keys.any?
    end

    def expected_policy_keys
      explicit_keys = Array(@intent['__policy_expected_keys']).map(&:to_s)
      filter_keys = Array(@intent['filters']).flat_map do |filter|
        [filter['param'], filter['param_start'], filter['param_end']]
      end.compact.map(&:to_s).select { |key| key.start_with?('policy_') }

      (explicit_keys + filter_keys).uniq
    end

    def effective_lint_allow_tables
      explicit_tables = Array(@allow_tables).compact.map(&:to_s).uniq
      policy_tables = Array(@intent['__policy_allowed_tables']).compact.map(&:to_s).uniq
      normalized_explicit_tables = explicit_tables.map(&:downcase).uniq
      normalized_policy_tables = policy_tables.map(&:downcase).uniq

      return @allow_tables if explicit_tables.empty? && policy_tables.empty?
      return normalized_policy_tables if explicit_tables.empty?
      return normalized_explicit_tables if policy_tables.empty?

      normalized_explicit_tables & normalized_policy_tables
    end

    def allowlist_sources_present?
      explicit_tables = Array(@allow_tables).compact
      policy_tables = Array(@intent['__policy_allowed_tables']).compact

      explicit_tables.any? || policy_tables.any?
    end

    def lint_sql!
      if allowlist_sources_present? && Array(effective_lint_allow_tables).empty?
        raise SecurityError, 'No tables remain after intersecting explicit and policy allowlists'
      end

      Guardrails::SqlLinter.new(@config, allow_tables: effective_lint_allow_tables).check!(@sql)
      check_top_level_table_allowlist!
      check_policy_scoped_related_table_references!
    end

    def check_top_level_table_allowlist!
      allowed_tables = Array(effective_lint_allow_tables).compact.map { |table| table.to_s.downcase }
      return if allowed_tables.empty?

      top_level_sql = strip_exists_subqueries(@sql)

      raise SecurityError, 'Top-level common table expressions are not allowed' if top_level_sql.match?(/\A\s*WITH\b/i)
      raise SecurityError, 'Top-level derived tables are not allowed' if top_level_sql.match?(/(?:\bFROM\b|\bJOIN\b|,)\s*(?:LATERAL\s+)?\(/i)

      extract_table_names(top_level_sql).each do |table|
        next if allowed_tables.include?(table.to_s.downcase)

        raise SecurityError, "Table '#{table}' is not in the allowed list: #{allowed_tables.join(', ')}"
      end
    end

    def check_policy_scoped_related_table_references!
      policy_scoped_related_tables = declared_related_tables
      return if policy_scoped_related_tables.empty?

      extract_table_names(strip_exists_subqueries(@sql)).each do |table|
        next unless policy_scoped_related_tables.include?(table.to_s.downcase)

        raise SecurityError,
              "Table '#{table}' is only allowed inside declared EXISTS/NOT EXISTS filters"
      end

      declared_references = Hash.new { |hash, key| hash[key] = [] }
      Array(@intent['filters']).each do |filter|
        operator = filter['op'].to_s.downcase
        table = filter['related_table']&.to_s&.downcase
        declared_references[[operator, table]] << filter if %w[exists not_exists].include?(operator) && table
      end
      policy_binds_by_declaration = policy_binds_by_related_filter

      sql_references = Hash.new(0)
      subqueries = sql_scanner.exists_subqueries(@sql)
      subqueries.each do |subquery|
        extract_table_names(strip_exists_subqueries(subquery[:sql])).each do |table|
          normalized_table = table.to_s.downcase
          next unless policy_scoped_related_tables.include?(normalized_table)

          reference = [subquery[:operator], normalized_table]
          sql_references[reference] += 1
          declaration = declared_references[reference][sql_references[reference] - 1]
          if declaration
            required_bind_numbers = policy_binds_by_declaration.fetch(declaration, [])
            next if policy_bind_present_in_subquery?(subquery, required_bind_numbers, subqueries)

            raise SecurityError,
                  "Table '#{table}' has an unscoped #{subquery[:operator].upcase} reference"
          end

          raise SecurityError,
                "Table '#{table}' has an undeclared #{subquery[:operator].upcase} reference"
        end
      end
    end

    def policy_bind_present_in_subquery?(subquery, policy_bind_numbers, subqueries)
      return true if policy_bind_numbers.empty? # This occurrence has no row predicate to enforce.

      nested_ranges = subqueries.filter_map do |candidate|
        next if candidate.equal?(subquery)
        next unless candidate[:start] >= subquery[:start] && candidate[:finish] <= subquery[:finish]

        candidate[:start]...candidate[:finish]
      end
      present_bind_numbers = sql_scanner.bind_placeholder_positions(@sql).filter_map do |position, bind_number|
        bind_number if position >= subquery[:start] && position < subquery[:finish] &&
                       nested_ranges.none? { |range| range.cover?(position) }
      end
      (policy_bind_numbers - present_bind_numbers).empty?
    end

    def policy_binds_by_related_filter
      declarations_by_table = Array(@intent['filters']).select do |filter|
        %w[exists not_exists].include?(filter['op'].to_s.downcase) && filter['related_table']
      end.group_by { |filter| filter['related_table'].to_s.downcase }

      declarations_by_table.each_with_object({}.compare_by_identity) do |(table, declarations), result|
        table_fragment = table.gsub(/[^a-zA-Z0-9_]/, '_')
        bind_numbers = Array(@bind_spec).each_with_index.filter_map do |bind, index|
          key = bind[:key]&.to_s
          index + 1 if key&.match?(/\Apolicy_subquery_\d+_#{Regexp.escape(table_fragment)}_/)
        end
        quotient, remainder = bind_numbers.length.divmod(declarations.length)
        offset = 0
        declarations.each_with_index do |declaration, index|
          count = quotient + (index < remainder ? 1 : 0)
          result[declaration] = bind_numbers.slice(offset, count)
          offset += count
        end
      end
    end

    def declared_related_tables
      Array(@intent['filters']).filter_map do |filter|
        op = filter['op'].to_s.downcase
        next unless %w[exists not_exists].include?(op)

        filter['related_table']&.to_s&.downcase
      end.compact.uniq
    end

    def strip_exists_subqueries(sql)
      sql_scanner.strip_exists_subqueries(sql)
    end

    def extract_table_names(sql)
      sql_scanner.extract_table_names(sql)
    end

    def sql_scanner
      @sql_scanner ||= SqlScanner.new
    end

    def infer_column_type(connection, table_name, column_name, explicit_cast, param_key = column_name)
      return explicit_cast if explicit_cast

      # Try to get column info from ActiveRecord
      if defined?(ActiveRecord::Base) && table_name
        begin
          model = infer_model_for_table(table_name)
          return model.column_for_attribute(column_name)&.type if model&.column_names&.include?(column_name.to_s)
        rescue StandardError
          # Fall through to connection-based lookup
        end

        # Fallback to direct connection query
        begin
          columns = connection.columns(table_name)
          column = columns.find { |c| c.name == column_name.to_s }
          return connection.lookup_cast_type_from_column(column) if column
        rescue StandardError
          # Fall through to type inference
        end
      end

      # Ultimate fallback: infer from parameter value
      infer_type_from_value(param_value_for_key(@params, param_key))
    end

    def infer_type_from_value(value)
      case value
      when Integer
        ActiveRecord::Type::Integer.new
      when Float
        ActiveRecord::Type::Decimal.new
      when Date
        ActiveRecord::Type::Date.new
      when Time, DateTime
        ActiveRecord::Type::DateTime.new
      when TrueClass, FalseClass
        ActiveRecord::Type::Boolean.new
      else
        ActiveRecord::Type::String.new
      end
    end

    def infer_model_for_table(table_name)
      return nil unless defined?(ActiveRecord::Base)
      return nil unless table_name

      # Try different naming conventions
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

    def apply_filter_to_scope(scope, filter)
      column = filter['column']
      operator = filter['op']

      case operator
      when '='
        param_key = filter['param'] || column
        value = param_value_for_key(@params, param_key)
        scope.where(column => value)
      when '!=', '<>'
        param_key = filter['param'] || column
        value = param_value_for_key(@params, param_key)
        scope.where.not(column => value)
      when '>', '>=', '<', '<='
        param_key = filter['param'] || column
        value = param_value_for_key(@params, param_key)
        scope.where("#{scope.connection.quote_column_name(column)} #{operator} ?", value)
      when 'between'
        start_key, end_key = between_filter_keys(filter)
        start_key, end_key = [start_key, end_key].map(&:to_s)
        start_value = param_value_for_key(@params, start_key)
        end_value = param_value_for_key(@params, end_key)
        start_value = param_value_for_key(@params, 'start') if !filter['param_start'] && !between_param_key_present?(@params, start_key)
        end_value = param_value_for_key(@params, 'end') if !filter['param_end'] && !between_param_key_present?(@params, end_key)
        scope.where(column => (start_value..end_value))
      when 'in'
        param_key = filter['param'] || column
        values = param_value_for_key(@params, param_key)
        scope.where(column => Array(values))
      when 'like', 'ilike'
        param_key = filter['param'] || column
        value = param_value_for_key(@params, param_key)
        scope.where("#{scope.connection.quote_column_name(column)} #{operator.upcase} ?", value)
      when 'exists', 'not_exists'
        related_table = filter['related_table']
        fk_column = filter['fk_column']
        base_column = filter['base_column'] || 'id'
        related_filters = Array(filter['related_filters'])

        unless related_table && fk_column
          warn "[code_to_query] Unsupported filter operator: #{operator}"
          return scope
        end

        # Use EXISTS subquery via where clause
        table_name = scope.klass.table_name
        subquery = scope.klass.unscoped
                        .from(related_table)
                        .where("#{related_table}.#{fk_column} = #{table_name}.#{base_column}")

        related_filters.each do |rf|
          rcol = rf['column']
          rop = rf['op']
          rkey = rf['param'] || rcol
          rval = param_value_for_key(@params, rkey)
          next if rcol.nil? || rop.nil?

          case rop
          when '=', '>', '<', '>=', '<=', '!=', '<>'
            subquery = if %w[!= <>].include?(rop)
                         subquery.where.not("#{related_table}.#{rcol} = ?", rval)
                       else
                         subquery.where("#{related_table}.#{rcol} #{rop} ?", rval)
                       end
          when 'between'
            start_key, end_key = between_filter_keys(rf)
            start_key, end_key = [start_key, end_key].map(&:to_s)
            start_val = param_value_for_key(@params, start_key)
            end_val = param_value_for_key(@params, end_key)
            start_val = param_value_for_key(@params, 'start') if !rf['param_start'] && !between_param_key_present?(@params, start_key)
            end_val = param_value_for_key(@params, 'end') if !rf['param_end'] && !between_param_key_present?(@params, end_key)
            subquery = subquery.where("#{related_table}.#{rcol} BETWEEN ? AND ?", start_val, end_val)
          when 'in'
            vals = Array(rval)
            subquery = subquery.where("#{related_table}.#{rcol} IN (?)", vals)
          when 'like', 'ilike'
            subquery = subquery.where("#{related_table}.#{rcol} #{rop.upcase} ?", rval)
          else
            warn "[code_to_query] Unsupported filter op in subquery: #{rop}"
          end
        end

        exists_sql = "EXISTS (#{subquery.select('1').to_sql})"
        if operator == 'not_exists'
          scope.where("NOT #{exists_sql}")
        else
          scope.where(exists_sql)
        end
      else
        warn "[code_to_query] Unsupported filter operator: #{operator}"
        scope
      end
    end

    def determine_appropriate_limit
      # Explicit limit always takes precedence
      return @intent['limit'] if @intent['limit']

      # Determine query type and apply appropriate limit
      if has_aggregations?
        @config.aggregation_limit
      elsif has_exists_checks?
        @config.exists_limit
      elsif @intent['distinct']
        @config.distinct_limit
      else
        @config.default_limit
      end
    end

    def has_aggregations?
      @intent['aggregations']&.any? ||
        @intent['columns']&.any? { |col| col.to_s.match?(/count\(|sum\(|avg\(|max\(|min\(/i) }
    end

    def has_exists_checks?
      @intent['filters']&.any? { |filter| %w[exists not_exists].include?(filter['op']) }
    end

    def format_explain_result(result)
      case result
      when Array
        result.map do |row|
          case row
          when Hash
            row.values.join(' | ')
          when Array
            row.join(' | ')
          else
            row.to_s
          end
        end.join("\n")
      when String
        result
      else
        result.to_s
      end
    end
  end
end
