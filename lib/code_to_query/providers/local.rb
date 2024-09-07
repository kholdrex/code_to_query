# frozen_string_literal: true

module CodeToQuery
  module Providers
    class Local < Base
      def extract_intent(prompt:, schema:, allow_tables:)
        @prompt = prompt.to_s.strip
        @schema = schema || {}
        @allow_tables = Array(allow_tables).compact
        started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

        table = select_table
        table ||= 'main_table' # Back-compat default expected by specs

        intent = {
          'type' => 'select',
          'table' => table,
          'columns' => ['*'],
          'filters' => [],
          'order' => [],
          'params' => {}
        }

        # Only set limit when configured (avoid nil which fails validation)
        intent['limit'] = @config.default_limit if @config.default_limit

        result = validate_and_enhance_intent(intent, allow_tables)

        # Lightweight metrics: elapsed and estimated tokens from prompt + schema table names
        elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
        prompt_blob = build_prompt_blob(@prompt, @schema)
        est = estimate_tokens(prompt_blob)
        @metrics[:prompt_tokens] = est
        @metrics[:completion_tokens] = 0
        @metrics[:total_tokens] = est
        @metrics[:elapsed_s] = elapsed

        result
      end

      private

      def build_prompt_blob(prompt, schema)
        tables = Array(schema['tables'] || schema.dig('schema', 'tables') || [])
        table_names = tables.map { |t| t['name'] || t[:name] }.compact.join(',')
        [prompt.to_s, table_names].join("\n")
      end

      def estimate_tokens(text)
        (text.to_s.length / 4.0).ceil
      end

      def select_table
        return @allow_tables.first if @allow_tables.any?

        tables = extract_schema_tables
        return tables.first[:name] if tables.any?

        nil
      end

      def extract_schema_tables
        return [] unless @schema.is_a?(Hash)

        raw_tables = if @schema['tables'].is_a?(Array)
                       @schema['tables']
                     elsif @schema['schema'].is_a?(Hash) && @schema['schema']['tables'].is_a?(Array)
                       @schema['schema']['tables']
                     else
                       []
                     end

        Array(raw_tables).map do |table|
          next unless table.is_a?(Hash)

          {
            name: table['name'] || table[:name],
            columns: Array(table['columns'] || table[:columns])
          }
        end.compact
      end
    end
  end
end
