# frozen_string_literal: true

begin
  require 'active_record'
rescue LoadError
end

module CodeToQuery
  class Query
    attr_reader :sql, :params, :intent, :metrics

    def initialize(sql:, params:, bind_spec:, intent:, allow_tables:, config:)
      @sql = sql
      @params = params || {}
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
        value = @params[key.to_s] || @params[key.to_sym]

        # Determine the correct ActiveRecord type
        type = infer_column_type(connection, @intent['table'], column_name, bind_info[:cast])

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
      return 'EXPLAIN unavailable (ActiveRecord not loaded)' unless defined?(ActiveRecord::Base)

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
    rescue StandardError => e
      "EXPLAIN failed: #{e.message}"
    end

    def to_relation
      return nil unless defined?(ActiveRecord::Base)
      return nil unless @intent['type'] == 'select'

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

      !!infer_model_for_table(@intent['table'])
    end

    def to_relation!
      rel = to_relation
      return rel if rel

      raise CodeToQuery::NotRelationConvertibleError, 'Query cannot be expressed as ActiveRecord::Relation'
    end

    def run
      Runner.new(@config).run(sql: @sql, binds: binds)
    end

    private

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
      Guardrails::SqlLinter.new(@config, allow_tables: @allow_tables).check!(@sql)

      # EXPLAIN-based performance checks
      return false if @config.enable_explain_gate && !Guardrails::ExplainGate.new(@config).allowed?(@sql)

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

      policy_in_binds = Array(@bind_spec).any? do |bind|
        key = bind[:key]
        key.to_s.start_with?('policy_')
      end

      policy_in_params = @params.keys.any? { |k| k.to_s.start_with?('policy_') }

      policy_in_binds || policy_in_params
    end

    def infer_column_type(connection, table_name, column_name, explicit_cast)
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
      infer_type_from_value(@params[column_name] || @params[column_name.to_sym])
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
        value = @params[param_key.to_s] || @params[param_key.to_sym]
        scope.where(column => value)
      when '!=', '<>'
        param_key = filter['param'] || column
        value = @params[param_key.to_s] || @params[param_key.to_sym]
        scope.where.not(column => value)
      when '>', '>=', '<', '<='
        param_key = filter['param'] || column
        value = @params[param_key.to_s] || @params[param_key.to_sym]
        scope.where("#{scope.connection.quote_column_name(column)} #{operator} ?", value)
      when 'between'
        start_key = filter['param_start'] || 'start'
        end_key = filter['param_end'] || 'end'
        start_value = @params[start_key.to_s] || @params[start_key.to_sym]
        end_value = @params[end_key.to_s] || @params[end_key.to_sym]
        scope.where(column => start_value..end_value)
      when 'in'
        param_key = filter['param'] || column
        values = @params[param_key.to_s] || @params[param_key.to_sym]
        scope.where(column => Array(values))
      when 'like', 'ilike'
        param_key = filter['param'] || column
        value = @params[param_key.to_s] || @params[param_key.to_sym]
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
          rval = @params[rkey.to_s] || @params[rkey.to_sym]
          next if rcol.nil? || rop.nil?

          case rop
          when '=', '>', '<', '>=', '<=', '!=', '<>'
            subquery = if %w[!= <>].include?(rop)
                         subquery.where.not("#{related_table}.#{rcol} = ?", rval)
                       else
                         subquery.where("#{related_table}.#{rcol} #{rop} ?", rval)
                       end
          when 'between'
            start_key = rf['param_start'] || 'start'
            end_key = rf['param_end'] || 'end'
            start_val = @params[start_key.to_s] || @params[start_key.to_sym]
            end_val = @params[end_key.to_s] || @params[end_key.to_sym]
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
