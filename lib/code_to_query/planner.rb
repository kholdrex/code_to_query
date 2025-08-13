# frozen_string_literal: true

module CodeToQuery
  class Planner
    def initialize(config)
      @config = config
    end

    def plan(prompt:, schema:, allow_tables:)
      schema ||= load_context_pack
      allow_tables = Array(allow_tables).compact

      attempt = 0
      last_error = nil
      feedback = nil
      provider = build_provider
      max_attempts = @config.planner_max_attempts || 1

      while attempt < max_attempts
        attempt += 1
        begin
          intent = provider.extract_intent(
            prompt: build_prompt_with_feedback(prompt, feedback),
            schema: schema,
            allow_tables: allow_tables
          )

          # Optional schema strictness pass: drop filters referencing unknown columns
          if @config.planner_feedback_mode.to_s == 'schema_strict'
            intent = strip_unknown_columns(intent, schema)
          end

          # Expose provider metrics if available
          if provider.respond_to?(:metrics) && provider.metrics.is_a?(Hash)
            intent = intent.merge('_metrics' => provider.metrics)
          end

          # Heuristic backfill of missing params from the prompt (IDs and enum-like labels)
          intent = backfill_params_from_prompt(prompt, intent, schema)

          return intent
        rescue StandardError => e
          last_error = e
          feedback = generate_feedback(e)
          @config.logger.warn("[code_to_query] Planning attempt #{attempt} failed: #{e.message}")
        end
      end

      @config.logger.warn("[code_to_query] Query planning failed after #{max_attempts} attempts: #{last_error&.message}")
      fallback_intent(allow_tables)
    rescue StandardError => e
      @config.logger.warn("[code_to_query] Query planning failed: #{e.message}")
      fallback_intent(allow_tables)
    end

    private

    def build_provider
      case @config.provider
      when :openai
        Providers::OpenAI.new(@config)
      when :local
        Providers::Local.new(@config)
      else
        detect_best_provider
      end
    end

    def detect_best_provider
      if @config.openai_api_key.present? && !@config.stub_llm
        Providers::OpenAI.new(@config)
      else
        Providers::Local.new(@config)
      end
    end

    def load_context_pack
      path = @config.context_pack_path
      unless File.exist?(path)
        begin
          # Attempt to bootstrap a context pack automatically if missing
          if defined?(CodeToQuery::Context::Builder)
            CodeToQuery::Context::Builder.new(@config).bootstrap!
          end
        rescue StandardError => e
          @config.logger.warn("[code_to_query] Auto-bootstrap of context pack failed: #{e.message}")
        end
      end

      return {} unless File.exist?(path)

      JSON.parse(File.read(path))
    rescue StandardError => e
      @config.logger.warn("[code_to_query] Failed to load context pack: #{e.message}")
      {}
    end

    def fallback_intent(allow_tables)
      intent = {
        'type' => 'select',
        'table' => Array(allow_tables).compact.first || 'main_table',
        'columns' => ['*'],
        'filters' => [],
        'order' => [],
        'params' => {}
      }
      intent['limit'] = @config.default_limit if @config.default_limit
      intent
    end

    def build_prompt_with_feedback(prompt, feedback)
      return prompt if feedback.to_s.strip.empty?

      "#{prompt}\n\nConstraints/feedback: #{feedback}"
    end

    def generate_feedback(error)
      return '' unless @config.planner_feedback_mode && @config.planner_feedback_mode != :none

      case error
      when ArgumentError
        'Ensure all columns and tables exist in the provided schema and avoid unknown fields. For EXISTS, provide related_table and fk_column.'
      else
        'Avoid inventing tables/columns; map business terms to schema; use EXISTS for relationships; set column to "id" in EXISTS filters.'
      end
    end

    def strip_unknown_columns(intent, schema)
      return intent unless intent.is_a?(Hash)

      tables = Array(schema.dig('schema', 'tables') || schema['tables'] || [])
      table_name = intent['table']
      table = tables.find { |t| (t['name'] || t[:name]).to_s == table_name.to_s }
      return intent unless table

      columns = Array(table['columns'] || table[:columns]).map { |c| c['name'] || c[:name] }.compact

      if intent['filters'].is_a?(Array)
        intent['filters'] = intent['filters'].select do |f|
          op = f['op'].to_s
          next true if %w[exists not_exists].include?(op)

          columns.include?(f['column'].to_s)
        end
      end

      if intent['order'].is_a?(Array)
        intent['order'] = intent['order'].select { |o| columns.include?(o['column'].to_s) }
      end

      if intent['columns'].is_a?(Array)
        intent['columns'] = intent['columns'].select { |c| c == '*' || columns.include?(c.to_s) }
      end

      intent
    rescue StandardError
      intent
    end

    # Fill missing intent params with simple values parsed from the prompt.
    # Example: "student with id 17963" -> maps first integer to first missing *_id param.
    def backfill_params_from_prompt(prompt, intent, schema)
      return intent unless prompt.is_a?(String)
      return intent unless intent.is_a?(Hash)

      numbers = begin
        prompt.scan(/\b\d+\b/).map { |n| Integer(n) }
      rescue StandardError
        []
      end
      return intent if numbers.empty?

      params = (intent['params'] || {}).dup
      remaining = numbers.dup

      # Helper to assign a value for a filter if missing
      assign_for = lambda do |param_key|
        return if param_key.nil? || param_key.to_s.strip.empty?
        return if params.key?(param_key.to_s)

        value = remaining.shift
        params[param_key.to_s] = value if value
      end

      # Main table filters first, prioritize *_id columns
      Array(intent['filters']).each do |f|
        op = f['op'].to_s
        next unless ['=', 'like', 'ilike', 'in', 'between'].include?(op) || %w[exists not_exists].include?(op)

        if %w[exists not_exists].include?(op)
          Array(f['related_filters']).each do |rf|
            pkey = rf['param'] || rf['column']
            if rf['column'].to_s.end_with?('_id')
              assign_for.call(pkey)
            end
          end
        else
          pkey = f['param'] || f['column']
          if f['column'].to_s.end_with?('_id')
            assign_for.call(pkey)
          end
        end
      end

      # Enum-like backfill using model enums from schema (if available)
      tokens = prompt.scan(/[a-z0-9_]+/i).map(&:downcase)
      enums_by_table = extract_enums_index(schema)

      # Main table columns
      table_name = intent['table']
      if table_name && enums_by_table[table_name]
        Array(intent['filters']).each do |f|
          next unless f.is_a?(Hash)
          next unless f['op'].to_s == '='

          col = f['column']
          next unless col

          pkey = (f['param'] || col).to_s
          next if params.key?(pkey)

          mapping = enums_by_table[table_name][col]
          next unless mapping.is_a?(Hash) && mapping.any?

          label = tokens.find { |tk| mapping.key?(tk) }
          params[pkey] = label if label
        end
      end

      # Related table columns in EXISTS
      Array(intent['filters']).each do |f|
        next unless f.is_a?(Hash) && %w[exists not_exists].include?(f['op'].to_s)

        rtable = f['related_table']
        next unless rtable && enums_by_table[rtable]

        Array(f['related_filters']).each do |rf|
          next unless rf.is_a?(Hash) && rf['op'].to_s == '='

          rcol = rf['column']
          next unless rcol

          rpkey = (rf['param'] || rcol).to_s
          next if params.key?(rpkey)

          rmapping = enums_by_table[rtable][rcol]
          next unless rmapping.is_a?(Hash) && rmapping.any?

          rlabel = tokens.find { |tk| rmapping.key?(tk) }
          params[rpkey] = rlabel if rlabel
        end
      end

      intent.merge('params' => params)
    rescue StandardError
      intent
    end

    def extract_enums_index(schema)
      index = Hash.new { |h, k| h[k] = {} }
      return index unless schema.is_a?(Hash)

      models = schema.dig('models', 'models')
      return index unless models.is_a?(Hash)

      models.each_value do |meta|
        next unless meta.is_a?(Hash)

        table = meta['table_name'] || meta[:table_name]
        next unless table

        enums = meta['enums'] || meta[:enums] || {}
        next unless enums.is_a?(Hash)

        enums.each do |col, mapping|
          # Normalize mapping keys to downcased strings
          next unless mapping.is_a?(Hash)

          norm = {}
          mapping.each { |k, v| norm[k.to_s.downcase] = v }
          index[table.to_s][col.to_s] = norm
        end
      end
      index
    rescue StandardError
      {}
    end
  end
end
