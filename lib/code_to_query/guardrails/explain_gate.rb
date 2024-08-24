# frozen_string_literal: true

begin
  require 'active_record'
rescue LoadError
end

module CodeToQuery
  module Guardrails
    class ExplainGate
      DEFAULT_MAX_COST = 10_000
      DEFAULT_MAX_ROWS = 100_000

      def initialize(config)
        @config = config
      end

      def allowed?(sql)
        return true unless defined?(ActiveRecord::Base) && ActiveRecord::Base.connected?

        plan = get_explain_plan(sql)
        return true if plan.nil? || plan.empty?

        analyze_plan_safety(plan)
      rescue StandardError => e
        # Log error; fail-open or fail-closed based on configuration
        CodeToQuery.config.logger.warn("[code_to_query] ExplainGate error: #{e.message}")
        !!@config.explain_fail_open
      end

      private

      def get_explain_plan(sql)
        explain_sql = build_explain_query(sql)

        result = if @config.readonly_role && ActiveRecord.respond_to?(:connected_to)
                   ActiveRecord::Base.connected_to(role: @config.readonly_role) do
                     ActiveRecord::Base.connection.execute(explain_sql)
                   end
                 else
                   ActiveRecord::Base.connection.execute(explain_sql)
                 end

        normalize_explain_result(result)
      end

      def build_explain_query(sql)
        case @config.adapter
        when :postgres, :postgresql
          "EXPLAIN (ANALYZE false, BUFFERS false, VERBOSE false, FORMAT JSON) #{sql}"
        when :mysql
          "EXPLAIN FORMAT=JSON #{sql}"
        when :sqlite
          "EXPLAIN QUERY PLAN #{sql}"
        else
          "EXPLAIN #{sql}"
        end
      end

      def normalize_explain_result(result)
        case result
        when Array
          result.map do |row|
            case row
            when Hash
              row
            when Array
              row.first
            else
              row.to_s
            end
          end
        else
          [result.to_s]
        end
      end

      def analyze_plan_safety(plan)
        case @config.adapter
        when :postgres, :postgresql
          analyze_postgres_plan(plan)
        when :mysql
          analyze_mysql_plan(plan)
        when :sqlite
          analyze_sqlite_plan(plan)
        else
          analyze_generic_plan(plan)
        end
      end

      def analyze_postgres_plan(plan)
        if plan.first.is_a?(Hash) && plan.first['QUERY PLAN']
          json_plan = plan.first['QUERY PLAN']
          return analyze_postgres_json_plan(json_plan)
        end

        analyze_postgres_text_plan(plan)
      end

      def analyze_postgres_json_plan(json_plan)
        return true unless json_plan.is_a?(Array) && json_plan.first.is_a?(Hash)

        root_node = json_plan.first['Plan']
        return true unless root_node

        # Check for expensive operations
        check_node_safety(root_node)
      end

      def check_node_safety(node)
        return true unless node.is_a?(Hash)

        node_type = node['Node Type']
        total_cost = node['Total Cost'].to_f if node['Total Cost']
        node['Startup Cost']&.to_f
        plan_rows = node['Plan Rows'].to_i if node['Plan Rows']

        # Block if costs are too high
        max_cost = @config.max_query_cost || DEFAULT_MAX_COST
        if total_cost && total_cost > max_cost
          warn "[code_to_query] Query blocked: total cost #{total_cost} exceeds limit #{max_cost}"
          return false
        end

        # Block if estimated rows are too high
        max_rows = @config.max_query_rows || DEFAULT_MAX_ROWS
        if plan_rows && plan_rows > max_rows
          warn "[code_to_query] Query blocked: estimated rows #{plan_rows} exceeds limit #{max_rows}"
          return false
        end

        # Block dangerous scan types
        case node_type
        when 'Seq Scan'
          # Only allow seq scans on small tables or if explicitly allowed
          if plan_rows && plan_rows > 1000 && !@config.allow_seq_scans
            warn "[code_to_query] Query blocked: sequential scan on large table (#{plan_rows} rows)"
            return false
          end
        when 'Nested Loop'
          # Block nested loops with high row estimates
          if plan_rows && plan_rows > 10_000
            warn "[code_to_query] Query blocked: expensive nested loop (#{plan_rows} rows)"
            return false
          end
        end

        # Recursively check child nodes
        node['Plans']&.each do |child_node|
          return false unless check_node_safety(child_node)
        end

        true
      end

      def analyze_postgres_text_plan(plan)
        plan_text = plan.join("\n").downcase

        if plan_text.include?('seq scan') && !@config.allow_seq_scans
          CodeToQuery.config.logger.warn('[code_to_query] Query blocked: contains sequential scan')
          return false
        end

        expensive_operations = [
          'sort',
          'hash join',
          'nested loop'
        ]

        expensive_operations.each do |op|
          if plan_text.include?(op) && plan_text.match?(/cost=\d{4,}/)
            CodeToQuery.config.logger.warn("[code_to_query] Query blocked: expensive #{op} operation")
            return false
          end
        end

        true
      end

      def analyze_mysql_plan(plan)
        plan_text = plan.join("\n").downcase

        if plan_text.include?('full table scan')
          CodeToQuery.config.logger.warn('[code_to_query] Query blocked: full table scan detected')
          return false
        end

        if plan_text.include?('using filesort')
          CodeToQuery.config.logger.warn('[code_to_query] Query blocked: filesort operation detected')
          return false
        end

        true
      end

      def analyze_sqlite_plan(plan)
        plan_text = plan.join("\n").downcase

        if plan_text.include?('scan table')
          CodeToQuery.config.logger.warn('[code_to_query] Query blocked: table scan detected')
          return false
        end

        true
      end

      def analyze_generic_plan(plan)
        plan_text = plan.join("\n").downcase

        dangerous_patterns = [
          /full.+scan/,
          /seq.+scan/,
          /table.+scan/,
          /sort.+\d{4,}/,  # Expensive sorts
          /cost.+\d{4,}/   # High cost operations
        ]

        dangerous_patterns.each do |pattern|
          if plan_text.match?(pattern)
            CodeToQuery.config.logger.warn("[code_to_query] Query blocked: dangerous pattern #{pattern} in plan")
            return false
          end
        end

        true
      end
    end
  end
end
