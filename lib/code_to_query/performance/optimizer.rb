# frozen_string_literal: true

require 'digest'

module CodeToQuery
  module Performance
    # Query optimization and performance analysis
    class Optimizer
      def initialize(config)
        @config = config
        @query_stats = {}
      end

      # Analyze and optimize a query before execution
      def optimize_query(sql, intent = {})
        start_time = Time.now

        analysis = analyze_query(sql, intent)
        optimized_sql = apply_optimizations(sql, analysis)

        optimization_time = Time.now - start_time

        {
          original_sql: sql,
          optimized_sql: optimized_sql,
          analysis: analysis,
          optimization_time: optimization_time,
          recommendations: generate_recommendations(analysis)
        }
      end

      # Performance monitoring for executed queries
      def track_query_performance(sql, execution_time, result_count = 0)
        query_hash = Digest::SHA256.hexdigest(sql)

        @query_stats[query_hash] ||= {
          sql: sql,
          execution_count: 0,
          total_time: 0.0,
          min_time: Float::INFINITY,
          max_time: 0.0,
          avg_time: 0.0,
          last_executed: nil,
          result_counts: []
        }

        stats = @query_stats[query_hash]
        prev_count = stats[:execution_count]
        prev_total = stats[:total_time]
        prev_avg = prev_count.positive? ? (prev_total / prev_count) : 0.0

        stats[:execution_count] += 1
        stats[:total_time] += execution_time
        stats[:min_time] = [stats[:min_time], execution_time].min
        stats[:max_time] = [stats[:max_time], execution_time].max
        stats[:avg_time] = stats[:total_time] / stats[:execution_count]
        stats[:last_executed] = Time.now
        stats[:result_counts] << result_count

        # Alert on performance degradation relative to previous average
        if prev_count >= 5 && execution_time > (prev_avg * 3)
          message = '[code_to_query] PERFORMANCE ALERT: Query performance degrading'
          CodeToQuery.config.logger.warn(message)
          warn message
        end

        # Alert on slow queries based on running average and other signals
        check_performance_alerts(stats)

        stats
      end

      # Get performance insights
      def performance_report
        {
          total_queries: @query_stats.size,
          most_frequent: most_frequent_queries,
          slowest: slowest_queries,
          fastest: fastest_queries,
          recommendations: global_recommendations
        }
      end

      private

      def analyze_query(sql, intent)
        {
          estimated_complexity: estimate_complexity(sql),
          join_count: count_joins(sql),
          has_subqueries: has_subqueries?(sql),
          has_aggregations: has_aggregations?(sql),
          has_order_by: has_order_by?(sql),
          has_group_by: has_group_by?(sql),
          limit_clause: extract_limit(sql),
          table_count: count_tables(sql),
          where_conditions: count_where_conditions(sql),
          potential_bottlenecks: identify_bottlenecks(sql),
          index_recommendations: suggest_indexes(sql, intent)
        }
      end

      def estimate_complexity(sql)
        complexity_score = 0

        # Base complexity
        complexity_score += 1

        # Joins add significant complexity
        join_count = count_joins(sql)
        complexity_score += join_count * 3 # Increased from 2

        # Subqueries add complexity
        complexity_score += sql.scan(/\(\s*SELECT/i).size * 4 # Increased from 3

        # Aggregations add complexity
        complexity_score += %w[SUM AVG COUNT MAX MIN].count { |func| sql.include?(func) } * 2

        # GROUP BY adds complexity
        complexity_score += 3 if has_group_by?(sql) # Increased from 2

        # ORDER BY adds complexity
        complexity_score += 1 if has_order_by?(sql)

        # Multiple tables add complexity
        table_count = count_tables(sql)
        complexity_score += (table_count - 1) if table_count > 1

        case complexity_score
        when 0..2
          :low
        when 3..6
          :medium
        when 7..12
          :high
        else
          :very_high
        end
      end

      def count_joins(sql)
        sql.scan(/\bJOIN\b/i).size
      end

      def has_subqueries?(sql)
        sql.match?(/\(\s*SELECT/i)
      end

      def has_aggregations?(sql)
        %w[SUM AVG COUNT MAX MIN].any? { |func| sql.match?(/\b#{func}\s*\(/i) }
      end

      def has_order_by?(sql)
        sql.match?(/\bORDER\s+BY\b/i)
      end

      def has_group_by?(sql)
        sql.match?(/\bGROUP\s+BY\b/i)
      end

      def extract_limit(sql)
        match = sql.match(/\bLIMIT\s+(\d+)/i)
        match ? match[1].to_i : nil
      end

      def count_tables(sql)
        # Simple approximation - count FROM and JOIN clauses
        from_count = sql.scan(/\bFROM\s+\w+/i).size
        join_count = count_joins(sql)
        [from_count + join_count, 1].max
      end

      def count_where_conditions(sql)
        where_match = sql.match(/\bWHERE\s+(.+?)(?:\s+ORDER\s+BY|\s+GROUP\s+BY|\s+LIMIT|\s*$)/i)
        return 0 unless where_match

        where_clause = where_match[1]
        # Count AND/OR operators as a proxy for condition complexity
        and_count = where_clause.scan(/\bAND\b/i).size
        or_count = where_clause.scan(/\bOR\b/i).size

        [and_count + or_count + 1, 1].max
      end

      def identify_bottlenecks(sql)
        bottlenecks = []

        bottlenecks << :missing_limit if extract_limit(sql).nil? || extract_limit(sql) > 1000
        bottlenecks << :too_many_joins if count_joins(sql) > 3
        bottlenecks << :complex_where if count_where_conditions(sql) > 5
        bottlenecks << :subqueries if has_subqueries?(sql)
        bottlenecks << :wildcard_select if sql.match?(/SELECT\s+\*/i)
        bottlenecks << :no_indexes if lacks_indexed_columns?(sql)

        bottlenecks
      end

      def lacks_indexed_columns?(sql)
        # Heuristic: if we're filtering on non-standard columns, might lack indexes
        where_match = sql.match(/\bWHERE\s+(.+?)(?:\s+ORDER|\s+GROUP|\s+LIMIT|\s*$)/i)
        return false unless where_match

        where_clause = where_match[1]
        # Common indexed columns
        indexed_columns = %w[id created_at updated_at]

        # If WHERE clause doesn't mention any commonly indexed columns
        indexed_columns.none? { |col| where_clause.match?(/\b#{col}\b/i) }
      end

      def suggest_indexes(sql, _intent)
        # Extract columns from WHERE clause
        where_columns = extract_where_columns(sql)
        suggestions = where_columns.map do |column|
          {
            type: :single_column,
            column: column,
            reason: 'Filtered in WHERE clause'
          }
        end

        # Extract columns from ORDER BY
        order_columns = extract_order_columns(sql)
        order_columns.each do |column|
          next if where_columns.include?(column)

          suggestions << {
            type: :single_column,
            column: column,
            reason: 'Used in ORDER BY'
          }
        end

        # Suggest composite indexes for multiple WHERE conditions
        if where_columns.size > 1
          suggestions << {
            type: :composite,
            columns: where_columns,
            reason: 'Multiple WHERE conditions'
          }
        end

        suggestions
      end

      def extract_where_columns(sql)
        where_match = sql.match(/\bWHERE\s+(.+?)(?:\s+ORDER|\s+GROUP|\s+LIMIT|\s*$)/i)
        return [] unless where_match

        where_clause = where_match[1]
        # Extract column names with improved regex patterns
        columns = []

        # Standard comparison operators
        columns += where_clause.scan(/\b([a-zA-Z_][a-zA-Z0-9_]*)\s*[=<>!]=?/).flatten

        # IN clauses
        columns += where_clause.scan(/\b([a-zA-Z_][a-zA-Z0-9_]*)\s+IN\s*\(/i).flatten

        # LIKE clauses
        columns += where_clause.scan(/\b([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:I?LIKE)/i).flatten

        # BETWEEN clauses
        columns += where_clause.scan(/\b([a-zA-Z_][a-zA-Z0-9_]*)\s+BETWEEN/i).flatten

        columns.uniq
      end

      def extract_order_columns(sql)
        order_match = sql.match(/\bORDER\s+BY\s+([^;]+)/i)
        return [] unless order_match

        order_clause = order_match[1]
        # Extract column names from ORDER BY
        columns = order_clause.scan(/\b([a-zA-Z_][a-zA-Z0-9_]*)\b/)
        columns.flatten.reject { |col| %w[ASC DESC].include?(col.upcase) }
      end

      def apply_optimizations(sql, analysis)
        optimized_sql = sql.dup

        # Add LIMIT if missing and query seems expensive
        if analysis[:limit_clause].nil? && (analysis[:estimated_complexity] != :low || analysis[:join_count].positive?)
          optimized_sql += " LIMIT #{@config.default_limit}"
        end

        # Suggest query restructuring for very complex queries
        if analysis[:estimated_complexity] == :very_high
          CodeToQuery.config.logger.warn('[code_to_query] Query complexity is very high, consider breaking into smaller queries')
        end

        optimized_sql
      end

      def generate_recommendations(analysis)
        recommendations = []

        case analysis[:estimated_complexity]
        when :high, :very_high
          recommendations << 'Consider breaking this query into smaller, simpler queries'
          recommendations << 'Review the necessity of all JOIN operations'
        end

        if analysis[:join_count] > 3
          recommendations << 'High number of JOINs detected - ensure proper indexing on join columns'
        end

        recommendations << 'Consider rewriting subqueries as JOINs for better performance' if analysis[:has_subqueries]

        if analysis[:limit_clause].nil?
          recommendations << 'Add LIMIT clause to prevent excessive result sets'
        elsif analysis[:limit_clause] > 1000
          recommendations << 'Consider reducing LIMIT value for better performance'
        end

        analysis[:potential_bottlenecks].each do |bottleneck|
          case bottleneck
          when :wildcard_select
            recommendations << 'Avoid SELECT * - specify only needed columns'
          when :no_indexes
            recommendations << 'Consider adding indexes on filtered columns'
          when :complex_where
            recommendations << 'Simplify WHERE clause conditions'
          end
        end

        recommendations
      end

      def check_performance_alerts(stats)
        # Alert on slow queries (> 1 second average)
        if stats[:avg_time] > 1.0
          CodeToQuery.config.logger.warn("[code_to_query] PERFORMANCE ALERT: Slow query detected (avg: #{stats[:avg_time].round(3)}s)")
        end

        # Alert on queries that are getting slower
        return unless stats[:execution_count] > 5 && stats[:max_time] > stats[:avg_time] * 3

        CodeToQuery.config.logger.warn('[code_to_query] PERFORMANCE ALERT: Query performance degrading')
      end

      def most_frequent_queries(limit = 5)
        @query_stats.values
                    .sort_by { |stats| -stats[:execution_count] }
                    .first(limit)
                    .map { |stats| format_query_stats(stats) }
      end

      def slowest_queries(limit = 5)
        @query_stats.values
                    .sort_by { |stats| -stats[:avg_time] }
                    .first(limit)
                    .map { |stats| format_query_stats(stats) }
      end

      def fastest_queries(limit = 5)
        @query_stats.values
                    .select { |stats| stats[:execution_count] > 1 }
                    .sort_by { |stats| stats[:avg_time] }
                    .first(limit)
                    .map { |stats| format_query_stats(stats) }
      end

      def format_query_stats(stats)
        {
          sql: safe_truncate(stats[:sql], 100),
          executions: stats[:execution_count],
          avg_time: stats[:avg_time].round(3),
          min_time: stats[:min_time].round(3),
          max_time: stats[:max_time].round(3)
        }
      end

      def safe_truncate(str, length)
        s = str.to_s
        return s if s.length <= length

        "#{s[0, length]}…"
      end

      def global_recommendations
        recommendations = []

        if @query_stats.any? { |_hash, stats| stats[:avg_time] > 1.0 }
          recommendations << 'Some queries are running slowly - consider optimization'
        end

        slow_query_count = @query_stats.count { |_hash, stats| stats[:avg_time] > 0.5 }
        if slow_query_count > @query_stats.size * 0.3
          recommendations << 'High percentage of slow queries - review indexing strategy'
        end

        recommendations
      end
    end
  end
end
