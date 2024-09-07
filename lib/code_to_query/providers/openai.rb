# frozen_string_literal: true

require 'net/http'
require 'json'
require 'uri'
require 'erb'

module CodeToQuery
  module Providers
    class OpenAI < Base
      API_BASE = 'https://api.openai.com/v1'

      def extract_intent(prompt:, schema:, allow_tables:)
        @schema = schema || {}
        @glossary = begin
          @schema['glossary'] || {}
        rescue StandardError
          {}
        end
        candidate_tables = select_context_tables(prompt, allow_tables)
        context = build_system_context(schema, candidate_tables)

        started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        response = make_api_request(
          messages: build_messages(prompt, context),
          functions: [intent_extraction_function],
          function_call: { name: 'extract_query_intent' }
        )
        elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at

        function_call = response.dig('choices', 0, 'message', 'function_call')
        intent_json = JSON.parse(function_call['arguments'])

        normalized = enhance_with_schema(intent_json, allow_tables: allow_tables, prompt_text: prompt)

        normalized['limit'] = @config.default_limit if @config.default_limit
        result = validate_and_enhance_intent(normalized, allow_tables)

        usage = response['usage'] || {}
        @metrics[:prompt_tokens] = usage['prompt_tokens']
        @metrics[:completion_tokens] = usage['completion_tokens']
        @metrics[:total_tokens] = usage['total_tokens']
        @metrics[:elapsed_s] = elapsed

        result
      end

      private

      def select_context_tables(prompt, allow_tables)
        top_k = (@config.context_rag_top_k || 0).to_i
        allow = Array(allow_tables).compact

        return allow if top_k <= 0

        ranked = rank_tables_for_prompt(prompt)
        chosen = ranked.take(top_k)
        if allow.any?
          chosen &= allow
          chosen = allow if chosen.empty?
        end
        chosen
      rescue StandardError
        Array(allow_tables).compact
      end

      def rank_tables_for_prompt(prompt)
        candidates = extract_schema_tables
        return [] unless candidates.any?

        text = prompt.to_s
        tokens = text.scan(/[a-z0-9_]+/i).map { |t| normalize_token(t) }.uniq

        candidates.map do |t|
          name = t[:name].to_s
          base = name_match_score(name, text)
          column_score = column_overlap_score(name, tokens)
          { table: name, score: (0.7 * base) + (0.3 * column_score) }
        end.sort_by { |h| -h[:score] }.map { |h| h[:table] }
      rescue StandardError
        []
      end

      def make_api_request(messages:, functions:, function_call:)
        base = (@config.llm_api_base || API_BASE).to_s
        uri = URI("#{base.chomp('/')}/chat/completions")

        payload = {
          model: @config.openai_model,
          messages: messages,
          temperature: @config.llm_temperature,
          functions: functions,
          function_call: function_call
        }.merge(@config.provider_options || {})

        response = http_request(uri, payload)

        raise "OpenAI API error: #{response.code} #{response.message}" unless response.is_a?(Net::HTTPSuccess)

        JSON.parse(response.body)
      end

      def enhance_with_schema(intent, allow_tables: [], prompt_text: nil)
        original_table = intent['table']
        if Array(allow_tables).any? && !Array(allow_tables).include?(original_table)
          return intent
        end

        resolved_table = pick_best_table(original_table)
        table_name = if Array(allow_tables).any?
                       Array(allow_tables).include?(resolved_table) ? resolved_table : original_table
                     else
                       resolved_table
                     end
        return intent unless table_name

        intent['table'] = table_name

        if intent['order'].is_a?(Array)
          intent['order'] = intent['order'].map do |ord|
            col = ord['column'] || ord[:column]
            dir = (ord['dir'] || ord[:dir] || 'desc').to_s
            resolved = resolve_column_name(col, table_name) || col
            { 'column' => resolved, 'dir' => dir }
          end
        end

        if intent['filters'].is_a?(Array)
          intent['filters'] = intent['filters'].map do |f|
            col = f['column'] || f[:column]
            resolved = resolve_column_name(col, table_name) || col
            f = f.merge('column' => resolved)
            if %w[exists not_exists].include?(f['op'].to_s) && f['related_filters'].is_a?(Array)
              f['related_filters'] = f['related_filters'].map do |rf|
                rcol = rf['column'] || rf[:column]
                rf.merge('column' => rcol)
              end
            end
            f
          end
        end

        if intent['columns'].is_a?(Array)
          intent['columns'] = intent['columns'].map do |c|
            resolve_column_name(c, table_name) || c
          end
        end

        # Backfill missing params for '=' filters using enum labels from prompt tokens
        if prompt_text.is_a?(String) && intent['params'].is_a?(Hash)
          tokens = prompt_text.scan(/[a-z0-9_]+/i).map { |t| normalize_token(t) }
          model_name = find_model_name_for_table(table_name)
          enums = model_enums(model_name)
          if enums.is_a?(Hash) && enums.any?
            intent['filters'].to_a.each do |f|
              next unless f.is_a?(Hash) && f['op'].to_s == '='

              col = f['column']
              next unless col

              pkey = (f['param'] || col).to_s
              next if intent['params'].key?(pkey)

              mapping = enums[col] || enums[col.to_s]
              next unless mapping.is_a?(Hash) && mapping.any?

              normalized_labels = mapping.keys.map { |k| normalize_token(k) }
              match_idx = normalized_labels.index { |lab| tokens.include?(lab) }
              if match_idx
                original_label = mapping.keys[match_idx]
                intent['params'][pkey] = original_label
              end
            end
          end

          # Fallback using glossary synonyms when enums are missing
          intent['filters'].to_a.each do |f|
            next unless f.is_a?(Hash) && f['op'].to_s == '='

            col = f['column']
            next unless col

            pkey = (f['param'] || col).to_s
            next if intent['params'].key?(pkey)

            syns = Array(@glossary["#{table_name}.#{col}"]) + Array(@glossary[col.to_s])
            next if syns.empty?

            norm_syns = syns.map { |s| normalize_token(s) }
            match_idx = norm_syns.index { |s| tokens.include?(s) }
            intent['params'][pkey] = syns[match_idx] if match_idx
          end
        end

        if intent['params'].is_a?(Hash)
          model_name = find_model_name_for_table(table_name)
          intent['filters'].to_a.each do |f|
            param_key = f['param'] || f['column']
            next unless param_key

            raw_value = intent['params'][param_key] || intent['params'][param_key.to_s] || intent['params'][param_key.to_sym]
            next if raw_value.nil?

            normalized = map_enum_like_value(model_name, f['column'], raw_value)
            unless normalized.equal?(raw_value)
              intent['params'][param_key] = normalized
            end
          end
        end

        if intent['params'].is_a?(Hash)
          model_name = find_model_name_for_table(table_name)
          intent['filters'].to_a.each do |f|
            col = f['column']
            param_key = f['param'] || col
            next unless param_key && col

            raw_value = intent['params'][param_key]
            next unless raw_value.is_a?(String)

            prev = intent['params'][param_key]
            mapped = map_enum_like_value(model_name, col, raw_value)
            intent['params'][param_key] = mapped if mapped != prev
          end
        end

        intent
      rescue StandardError
        intent
      end

      def pick_best_table(requested)
        candidates = extract_schema_tables
        return requested unless candidates.any?

        @schema['prompt_normalized'] # not present; fallback will compute below
        prompt_text = [requested.to_s].join(' ').downcase
        tokens = prompt_text.scan(/[a-z0-9_]+/).map { |t| normalize_token(t) }.uniq

        ranked = candidates.map do |t|
          name = t[:name].to_s
          base = name_match_score(name, requested.to_s)
          column_score = column_overlap_score(name, tokens)
          { table: name, score: (0.7 * base) + (0.3 * column_score) }
        end.sort_by { |h| -h[:score] }

        ranked.first[:table]
      rescue StandardError
        requested
      end

      def name_match_score(table_name, requested)
        p = requested.to_s.downcase
        return 0.0 if p.empty?
        return 1.0 if p.include?(table_name.to_s.downcase)

        singular = table_name.to_s.chomp('s')
        return 0.9 if p.include?(singular)

        # glossary table synonyms
        syns = Array(@glossary[table_name.to_s])
        return 0.8 if syns.any? { |s| p.include?(s.to_s.downcase) }

        # partial
        chunks = table_name.to_s.downcase.chars.each_cons(3).map(&:join)
        overlap = chunks.count { |c| p.include?(c) }
        [overlap.to_f / [table_name.length, 1].max * 0.6, 0.1].max
      end

      def column_overlap_score(table_name, tokens)
        return 0.0 if tokens.empty?

        cols = table_columns(table_name)
        terms = []
        cols.each do |c|
          pieces = c.to_s.downcase.split('_')
          terms.concat(pieces)
          terms.concat(Array(@glossary["#{table_name}.#{c}"]))
          pieces.each { |p| terms.concat(generic_token_synonyms(p)) }
        end
        terms = terms.compact.map { |t| normalize_token(t) }.uniq
        overlap = tokens & terms
        [(overlap.length.to_f / tokens.length), 1.0].min
      end

      def generic_token_synonyms(token)
        dict = {
          'score' => %w[rating grade ability level points],
          'ability' => %w[skill proficiency competency level score]
        }
        Array(dict[normalize_token(token)])
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

      def table_columns(table_name)
        tables = extract_schema_tables
        table = tables.find { |t| t[:name].to_s == table_name.to_s }
        return [] unless table

        Array(table[:columns]).map { |c| c['name'] || c[:name] }.compact
      end

      def resolve_column_name(requested_name, table_name)
        return nil if requested_name.nil? || table_name.nil?
        return nil if requested_name == '*'

        requested_norm = normalize_token(requested_name)
        return nil if requested_norm.empty?

        available = table_columns(table_name)
        return requested_name if available.include?(requested_name)

        available.each do |col|
          return col if normalize_token(col) == requested_norm

          glossary_key = "#{table_name}.#{col}"
          synonyms = Array(@glossary[glossary_key])
          synonyms.each do |syn|
            return col if normalize_token(syn) == requested_norm
          end
        end

        general_synonyms = Array(@glossary[requested_name]) + Array(@glossary[requested_norm]) + Array(@glossary[requested_name.to_s.downcase])
        general_synonyms.map { |s| normalize_token(s) }.uniq.each do |cand|
          available.each do |col|
            col_norm = normalize_token(col)
            return col if col_norm == cand || col_norm.include?(cand) || cand.include?(col_norm)
          end
        end

        if requested_norm.length >= 3
          available.each do |col|
            col_norm = normalize_token(col)
            return col if col_norm.include?(requested_norm) || requested_norm.include?(col_norm)
          end
        end

        nil
      end

      def normalize_token(str)
        str.to_s.downcase.gsub(/[^a-z0-9]/, '')
      end

      def find_model_name_for_table(table_name)
        models = @schema.dig('models', 'models')
        return nil unless models.is_a?(Hash)

        entry = models.find { |_mn, md| (md['table_name'] || md[:table_name]).to_s == table_name.to_s }
        entry&.first
      end

      def model_enums(model_name)
        @schema.dig('models', 'models', model_name, 'enums') || {}
      end

      def model_scopes(model_name)
        @schema.dig('models', 'scopes', model_name) || {}
      end

      def map_enum_like_value(model_name, column_name, raw_value)
        return raw_value if raw_value.is_a?(Numeric)
        return raw_value unless model_name && column_name

        str = raw_value.to_s
        return raw_value if str.empty?

        enums = model_enums(model_name)
        if enums.is_a?(Hash)
          mapping = enums[column_name] || enums[column_name.to_s]
          if mapping.is_a?(Hash)
            # Rails defined_enums use string keys and integer values
            mapped = mapping[str] || mapping[str.downcase]
            return Integer(mapped) if mapped
          end
        end

        # Fallback: infer from scopes where clause, e.g. scopes like with_videos -> WHERE attachment_type = 0
        scopes = model_scopes(model_name)
        if scopes.is_a?(Hash)
          token = normalize_token(str)
          scopes.each do |scope_name, meta|
            next unless meta.is_a?(Hash)

            where = meta['where'] || meta[:where]
            next unless where.is_a?(String)
            # Require the where to mention the target column
            next unless where.match?(/\b#{Regexp.escape(column_name)}\b\s*=\s*(\d+)/)

            # Heuristic match of scope name to token
            sn = normalize_token(scope_name.to_s)
            if sn.include?(token) || token.include?(sn)
              m = where.match(/\b#{Regexp.escape(column_name)}\b\s*=\s*(\d+)/)
              return Integer(m[1]) if m
            end
          end
        end

        raw_value
      end

      def http_request(uri, payload)
        # If a custom client is provided, use it (must respond to :chat and return content)
        if @config.llm_client.respond_to?(:chat)
          content = @config.llm_client.chat(messages: payload[:messages], options: payload.except(:messages))
          # Emulate Net::HTTP success + body with content stitched into OpenAI-like response
          fake = Struct.new(:code, :message, :body)
          return fake.new('200', 'OK', { choices: [{ message: { function_call: { arguments: content.to_s } } }] }.to_json)
        end

        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = (uri.scheme == 'https')
        http.read_timeout = @config.llm_timeout

        request = Net::HTTP::Post.new(uri)
        request['Authorization'] = "Bearer #{@config.openai_api_key}"
        request['Content-Type'] = 'application/json'
        request.body = payload.to_json

        http.request(request)
      end

      def build_messages(prompt, context)
        [
          {
            role: 'system',
            content: build_system_prompt(context)
          },
          {
            role: 'user',
            content: "Convert this natural language query into a structured intent: \"#{prompt}\""
          }
        ]
      end

      def build_system_prompt(context)
        if @config.system_prompt_template
          return render_template(@config.system_prompt_template, context)
        end

        app_kind = defined?(Rails) ? 'a Rails application' : 'a Ruby application'
        <<~PROMPT
          You are an expert SQL query planner for #{app_kind}. Convert natural language queries into structured JSON intent objects that can be safely compiled into parameterized SQL.

          Available tables: #{context[:available_tables].any? ? context[:available_tables].join(', ') : 'Any table in the schema'}

          Schema summary (columns and foreign keys):
          #{context[:schema_info]}

          Requirements:
          #{context[:constraints].map { |c| "- #{c}" }.join("\n")}

          Guidance:
          - Infer relationships from foreign key columns (e.g., *_id patterns)
          - Use the business glossary to understand domain terminology and map user language to database concepts
          - For relationship queries (e.g., "X by Y with id Z"), create EXISTS filters with proper related_table and related_filters
          - CRITICAL: Extract specific entity IDs from natural language - put descriptive names in "param" fields and actual values in "params" object
          - When users mention relationships, look for junction/bridge tables that connect entities
          - Use bound parameters and avoid string/numeric literals in WHERE clauses
          - Map business domain terms using the glossary provided in the schema
          - Do not invent tables or columns not present in the schema summary
          - For complex relationships, prefer EXISTS/NOT EXISTS over JOINs for better performance

          CRITICAL: For EXISTS/NOT EXISTS operations, analyze the schema to find the relationship and provide:
          - related_table: The bridge/junction table that connects the entities
          - fk_column: The foreign key column in that table pointing back to main table
          - base_column: Column in main table to join on (usually "id")
          - related_filters: Array of conditions to apply in the EXISTS subquery (use descriptive param names, not literal values)
          - column: MUST be "id" (never null) - this is a dummy value required by the schema

          Parameter Naming: Use descriptive names for "param" fields based on the schema context and put the actual values in the "params" object.
          CRITICAL: param fields must contain parameter names (like column names or descriptive identifiers), never literal values from the prompt.

          Generate queries that are safe, performant, and match user intent precisely.
        PROMPT
      end

      def render_template(template_str, context)
        ERB.new(template_str).result(binding)
      end

      def intent_extraction_function
        {
          name: 'extract_query_intent',
          description: 'Extract structured query intent from natural language, paying special attention to entity IDs and relationships',
          parameters: {
            type: 'object',
            properties: {
              type: {
                type: 'string',
                enum: ['select'],
                description: 'Type of query (only SELECT allowed)'
              },
              table: {
                type: 'string',
                description: 'Primary table to query'
              },
              columns: {
                type: 'array',
                items: { type: 'string' },
                description: 'Columns to select (* for all)'
              },
              filters: {
                type: 'array',
                description: 'Array of filter conditions. For relationship queries, create EXISTS filters. Each filter must have either a simple column condition OR a complete EXISTS structure.',
                items: {
                  type: 'object',
                  properties: {
                    column: { type: 'string', description: 'Column name for simple filters. For EXISTS/NOT EXISTS filters, use "id" as a dummy value (never null)' },
                    op: { type: 'string', enum: ['=', '!=', '<>', '>', '<', '>=', '<=', 'between', 'in', 'like', 'ilike', 'exists', 'not_exists'] },
                    param: { type: 'string', description: 'Parameter name (NEVER the literal value) - derive from column names or context' },
                    param_start: { type: 'string' },
                    param_end: { type: 'string' },
                    related_table: { type: 'string', description: 'REQUIRED for exists/not_exists: the table to check in the subquery' },
                    fk_column: { type: 'string', description: 'REQUIRED for exists/not_exists: foreign key column in related_table' },
                    base_column: { type: 'string', description: 'Column in main table to join on (use "id" if not specified)' },
                    related_filters: {
                      type: 'array',
                      description: 'REQUIRED for exists/not_exists: Additional conditions in the EXISTS subquery',
                      items: {
                        type: 'object',
                        properties: {
                          column: { type: 'string', description: 'Column name in the related table' },
                          op: { type: 'string', description: 'Operator (usually "=")' },
                          param: { type: 'string', description: 'Parameter name (NEVER literal value) - derive from column names or context' }
                        },
                        required: %w[column op param]
                      }
                    }
                  },
                  required: %w[column op]
                }
              },
              order: {
                type: 'array',
                items: {
                  type: 'object',
                  properties: {
                    column: { type: 'string' },
                    dir: { type: 'string', enum: %w[asc desc] }
                  },
                  required: %w[column dir]
                }
              },
              limit: {
                type: 'integer',
                minimum: 1,
                maximum: 10_000
              },
              params: {
                type: 'object',
                description: 'REQUIRED: Extract and include ALL literal values from the prompt. Map each param name to its actual value'
              }
            },
            required: %w[type table columns limit params]
          }
        }
      end
    end
  end
end
