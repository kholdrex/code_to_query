# frozen_string_literal: true

require 'dry/schema'

module CodeToQuery
  class Validator
    IntentSchema = Dry::Schema.Params do
      required(:type).filled(:string)
      required(:table).filled(:string)
      required(:columns).array(:string)
      optional(:filters).array(:hash) do
        optional(:column).maybe(:string)
        required(:op).filled(:string)
        optional(:param).filled(:string)
        optional(:param_start).filled(:string)
        optional(:param_end).filled(:string)
        # Optional fields to support correlated subqueries (NOT EXISTS)
        optional(:related_table).filled(:string)
        optional(:fk_column).filled(:string)
        optional(:base_column).filled(:string)
        optional(:related_filters).array(:hash) do
          required(:column).filled(:string)
          required(:op).filled(:string)
          optional(:param).filled(:string)
          optional(:param_start).filled(:string)
          optional(:param_end).filled(:string)
        end
      end
      optional(:order).array(:hash) do
        required(:column).filled(:string)
        required(:dir).filled(:string)
      end
      optional(:limit).filled(:integer)
      optional(:params).hash
      optional(:distinct).filled(:bool)
      optional(:distinct_on).array(:string)
      optional(:aggregations).array(:hash) do
        required(:type).filled(:string)
        optional(:column).filled(:string)
      end
      optional(:group_by).array(:string)
    end

    def validate(intent_hash, current_user: nil, allow_tables: nil)
      preprocessed = preprocess_exists_filters(intent_hash)

      if fetch_value(preprocessed, :limit).nil? && CodeToQuery.config.default_limit
        preprocessed = preprocessed.merge('limit' => CodeToQuery.config.default_limit)
      end

      result = IntentSchema.call(preprocessed)
      raise ArgumentError, "Invalid intent: #{result.errors.to_h}" unless result.success?

      validated = result.to_h

      original_metrics = intent_hash['_metrics'] || intent_hash[:_metrics]
      validated['_metrics'] = original_metrics if original_metrics.is_a?(Hash)

      Array(fetch_value(validated, :filters)).each_with_index do |f, idx|
        op = fetch_value(f, :op).to_s
        if %w[exists not_exists].include?(op)
          unless fetch_value(f, :related_table).to_s.strip != '' && fetch_value(f, :fk_column).to_s.strip != ''
            raise ArgumentError, "Invalid intent: filters[#{idx}] requires related_table and fk_column for #{op}"
          end

          assign_default_value(f, :base_column, 'id')
          assign_default_value(f, :column, 'id')
        else
          unless fetch_value(f, :column).to_s.strip != ''
            raise ArgumentError, "Invalid intent: filters[#{idx}].column must be filled"
          end
        end
      end
      enforce_allowlists!(validated, current_user: current_user, allow_tables: allow_tables)
      validated
    end

    private

    def fetch_value(hash, key)
      return unless hash.respond_to?(:key?) && hash.respond_to?(:[])

      return hash[key] if hash.key?(key)

      if key.is_a?(Symbol)
        string_key = key.to_s
        return hash[string_key] if hash.key?(string_key)
      end

      if key.is_a?(String)
        symbol_key = key.to_sym
        return hash[symbol_key] if hash.key?(symbol_key)
      end

      nil
    end

    def assign_default_value(hash, key, value)
      return unless hash.respond_to?(:key?) && hash.respond_to?(:[]=)
      return unless fetch_value(hash, key).nil?

      hash[key.to_sym] = value
    end

    def preprocess_exists_filters(intent_hash)
      intent = intent_hash.dup
      filters_key = intent.key?('filters') ? 'filters' : :filters

      if intent[filters_key].is_a?(Array)
        intent[filters_key] = intent[filters_key].map do |filter|
          if filter.is_a?(Hash) && %w[exists not_exists].include?(fetch_value(filter, :op).to_s) && fetch_value(filter, :column).nil?
            filter.merge('column' => 'id')
          else
            filter
          end
        end
      end

      intent
    end

    def enforce_allowlists!(intent, current_user:, allow_tables:)
      # Enforce table allowlist if provided (from user input)
      if Array(allow_tables).any?
        table = fetch_value(intent, :table)
        if (table.to_s.strip != '') && !Array(allow_tables).map { |t| t.to_s.downcase }.include?(table.to_s.downcase)
          raise ArgumentError, "Invalid intent: table '#{table}' not allowed"
        end
      end

      # Consult policy adapter for additional table/column allowlists
      adapter = CodeToQuery.config.policy_adapter
      return unless adapter.respond_to?(:call)

      policy_info = safe_call_policy_adapter(adapter, current_user, table: fetch_value(intent, :table), intent: intent)
      if policy_info.nil?
        return handle_policy_failure('Policy adapter returned nil') if policy_adapter_fail_open?

        raise CodeToQuery::PolicyAdapterError, 'Policy adapter returned nil'
      end
      unless policy_info.is_a?(Hash)
        message = "Policy adapter returned #{policy_info.class}, expected Hash"
        return handle_policy_failure(message) if policy_adapter_fail_open?

        raise CodeToQuery::PolicyAdapterError, message
      end

      allowed_tables = Array(fetch_value(policy_info, :allowed_tables)).map { |t| t.to_s.downcase }
      if allowed_tables.any?
        table = fetch_value(intent, :table)
        if (table.to_s.strip != '') && !allowed_tables.include?(table.to_s.downcase)
          raise ArgumentError, "Invalid intent: table '#{table}' not permitted by policy"
        end
      end

      allowed_columns = fetch_value(policy_info, :allowed_columns) || {}
      return if allowed_columns.nil? || allowed_columns.empty?

      # Normalize map keys to strings with lowercase table and column names
      normalized = {}
      allowed_columns.each do |tbl, cols|
        normalized[tbl.to_s.downcase] = Array(cols).map { |c| c.to_s.downcase }
      end

      main_table = fetch_value(intent, :table).to_s.downcase

      # Columns in SELECT
      Array(fetch_value(intent, :columns)).each do |col|
        next if col == '*'
        next unless normalized[main_table]&.any?
        unless normalized[main_table].include?(col.to_s.downcase)
          raise ArgumentError, "Invalid intent: selecting column '#{col}' not permitted on '#{main_table}'"
        end
      end

      # ORDER BY columns
      Array(fetch_value(intent, :order)).each do |o|
        col = fetch_value(o, :column)
        next if col.nil?
        next unless normalized[main_table]&.any?
        unless normalized[main_table].include?(col.to_s.downcase)
          raise ArgumentError, "Invalid intent: ordering by column '#{col}' not permitted on '#{main_table}'"
        end
      end

      # DISTINCT ON columns
      Array(fetch_value(intent, :distinct_on)).each do |col|
        next unless normalized[main_table]&.any?
        unless normalized[main_table].include?(col.to_s.downcase)
          raise ArgumentError, "Invalid intent: distinct_on column '#{col}' not permitted on '#{main_table}'"
        end
      end

      # GROUP BY
      Array(fetch_value(intent, :group_by)).each do |col|
        next unless normalized[main_table]&.any?
        unless normalized[main_table].include?(col.to_s.downcase)
          raise ArgumentError, "Invalid intent: group_by column '#{col}' not permitted on '#{main_table}'"
        end
      end

      # WHERE filters
      Array(fetch_value(intent, :filters)).each do |f|
        op = fetch_value(f, :op).to_s
        if %w[exists not_exists].include?(op)
          related_table = fetch_value(f, :related_table)
          rel_cols = normalized[related_table.to_s.downcase]
          next if rel_cols.nil? || rel_cols.empty?

          Array(fetch_value(f, :related_filters)).each do |rf|
            col = fetch_value(rf, :column)
            next if col.nil?
            unless rel_cols.include?(col.to_s.downcase)
              raise ArgumentError, "Invalid intent: filter column '#{col}' not permitted on '#{related_table}'"
            end
          end
        else
          col = fetch_value(f, :column)
          next if col.nil?

          cols = normalized[main_table]
          next if cols.nil? || cols.empty?
          unless cols.include?(col.to_s.downcase)
            raise ArgumentError, "Invalid intent: filter column '#{col}' not permitted on '#{main_table}'"
          end
        end
      end
    rescue CodeToQuery::PolicyAdapterError
      raise
    rescue StandardError => e
      # Re-raise as ArgumentError to keep validator contract
      raise ArgumentError, e.message
    end

    def safe_call_policy_adapter(adapter, current_user, table:, intent:)
      adapter.call(current_user, table: table, intent: intent)
    rescue ArgumentError
      begin
        adapter.call(current_user, table: table)
      rescue ArgumentError
        begin
          adapter.call(current_user)
        rescue StandardError => e
          return handle_policy_failure("Policy adapter failed: #{e.message}") if policy_adapter_fail_open?

          raise CodeToQuery::PolicyAdapterError, "Policy adapter failed: #{e.message}"
        end
      rescue StandardError => e
        return handle_policy_failure("Policy adapter failed: #{e.message}") if policy_adapter_fail_open?

        raise CodeToQuery::PolicyAdapterError, "Policy adapter failed: #{e.message}"
      end
    rescue StandardError => e
      if policy_adapter_fail_open?
        CodeToQuery.config.logger.warn("[code_to_query] Policy adapter failed: #{e.message}")
        return {}
      end

      raise CodeToQuery::PolicyAdapterError, "Policy adapter failed: #{e.message}"
    end

    def handle_policy_failure(message)
      CodeToQuery.config.logger.warn("[code_to_query] #{message}")
      {}
    end

    def policy_adapter_fail_open?
      CodeToQuery.config.respond_to?(:policy_adapter_fail_open) && CodeToQuery.config.policy_adapter_fail_open
    end
  end
end
