# frozen_string_literal: true

module CodeToQuery
  module Providers
    class Base
      attr_reader :metrics

      def initialize(config)
        @config = config
        @metrics = {}
      end

      def extract_intent(prompt:, schema:, allow_tables:)
        raise NotImplementedError, 'Subclasses must implement #extract_intent'
      end

      protected

      # rubocop:disable Metrics/PerceivedComplexity, Metrics/BlockNesting
      def build_system_context(schema, allow_tables)
        # Support passing either a raw schema hash ({'tables'=>[...]})
        # or a full context pack ({'schema'=>{...}, 'models'=>{...}, 'glossary'=>{...}})
        raw_schema = schema
        model_defs = nil
        scopes_map = nil
        glossary = {}
        if schema.is_a?(Hash) && schema['schema'].is_a?(Hash)
          raw_schema = schema['schema']
          if schema['models'].is_a?(Hash)
            model_defs = schema['models']['models']
            scopes_map = schema['models']['scopes']
          end
          glossary = schema['glossary'] || {}
        end

        available_tables = if allow_tables.any?
                             allow_tables
                           elsif raw_schema.is_a?(Hash) && raw_schema['tables']
                             raw_schema['tables'].map { |t| t['name'] || t[:name] }.compact
                           else
                             []
                           end

        schema_info = if raw_schema.is_a?(Hash) && raw_schema['tables']
                        raw_schema['tables'].map do |table|
                          table_name = table['name'] || table[:name]
                          cols = Array(table['columns'] || table[:columns])
                          col_names = cols.map { |c| c['name'] || c[:name] }.compact
                          fks = col_names.select { |c| c.end_with?('_id') }
                          col_summary = col_names.take(10).join(', ')
                          fk_summary = fks.any? ? " | fks: #{fks.join(', ')}" : ''

                          scope_summary = ''
                          if model_defs && scopes_map
                            # find model for this table
                            model_name, _def = model_defs.find { |_mn, md| (md['table_name'] || md[:table_name]) == table_name }
                            if model_name
                              scopes_for_model = scopes_map[model_name] || scopes_map[model_name.to_sym]
                              if scopes_for_model.is_a?(Hash) && scopes_for_model.any?
                                # Include compact where summaries when available
                                pairs = scopes_for_model.to_a.take(4).map do |(sname, meta)|
                                  w = meta['where'] || meta[:where]
                                  w ? "#{sname}: #{w}" : sname.to_s
                                end
                                scope_summary = pairs.any? ? " | scopes: #{pairs.join('; ')}" : ''
                              end
                              # include enum mapping if present
                              enums = model_defs.dig(model_name, 'enums') || model_defs.dig(model_name.to_sym, :enums) || {}
                              if enums.is_a?(Hash) && enums.any?
                                enum_pairs = enums.to_a.take(3).map do |(col, mapping)|
                                  sample = mapping.is_a?(Hash) ? mapping.to_a.take(2).map { |k, v| "#{k}=#{v}" }.join(', ') : ''
                                  sample.empty? ? col.to_s : "#{col}(#{sample})"
                                end
                                scope_summary += enum_pairs.any? ? " | enums: #{enum_pairs.join('; ')}" : ''
                              end
                            end
                          end

                          "#{table_name}: #{col_summary}#{fk_summary}#{scope_summary}"
                        end.join("\n")
                      else
                        'No schema information available'
                      end

        # Include glossary information for better business understanding
        glossary_info = if glossary.any?
                          "\nBusiness Glossary (user terms -> database concepts):\n" \
                            "#{glossary.map { |term, synonyms| "#{term}: #{Array(synonyms).join(', ')}" }.join("\n")}"
                        else
                          ''
                        end

        {
          available_tables: available_tables,
          schema_info: schema_info + glossary_info,
          constraints: build_constraints_info
        }
      end
      # rubocop:enable Metrics/PerceivedComplexity, Metrics/BlockNesting

      def build_constraints_info
        base_constraints = [
          'Only generate SELECT queries',
          'All values must be parameterized',
          'Use standard SQL operators: =, !=, <>, >, <, >=, <=, between, in, like, ilike',
          'Prefer EXISTS/NOT EXISTS when expressing presence/absence of related rows',
          'DISTINCT is allowed for unique result sets',
          'DISTINCT ON (columns) is supported for PostgreSQL',
          "For 'top N' queries, use ORDER BY with LIMIT"
        ]

        limit_constraints = build_limit_constraints
        aggregation_constraints = build_aggregation_constraints

        base_constraints + limit_constraints + aggregation_constraints
      end

      def build_limit_constraints
        constraints = []

        if @config.default_limit
          constraints << "Default LIMIT: #{@config.default_limit} for SELECT queries"
        end

        constraints << if @config.count_limit
                         "COUNT queries limited to #{@config.count_limit} rows"
                       else
                         'COUNT queries have no automatic LIMIT'
                       end

        constraints << if @config.aggregation_limit
                         "Aggregation queries limited to #{@config.aggregation_limit} rows"
                       else
                         'Aggregation queries (SUM, AVG, MAX, MIN) have no automatic LIMIT'
                       end

        if @config.distinct_limit
          constraints << "DISTINCT queries limited to #{@config.distinct_limit} rows"
        end

        if @config.exists_limit
          constraints << "EXISTS checks automatically use LIMIT #{@config.exists_limit}"
        end

        constraints
      end

      def build_aggregation_constraints
        [
          'COUNT(*) and COUNT(column) are supported',
          'SUM, AVG, MAX, MIN aggregations are supported',
          'GROUP BY is supported for aggregations',
          'Multiple aggregations can be combined in a single query'
        ]
      end

      def validate_and_enhance_intent(intent, allow_tables)
        intent['type'] ||= 'select'
        intent['columns'] ||= ['*']
        intent['filters'] ||= []
        intent['order'] ||= []
        intent['limit'] ||= @config.default_limit if @config.default_limit
        intent['params'] ||= {}

        if allow_tables.any? && !allow_tables.include?(intent['table'])
          raise ArgumentError, "Table '#{intent['table']}' not in allowlist: #{allow_tables.join(', ')}"
        end

        intent
      end
    end
  end
end
