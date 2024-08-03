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

      if !preprocessed.key?('limit') && CodeToQuery.config.default_limit
        preprocessed = preprocessed.merge('limit' => CodeToQuery.config.default_limit)
      end

      result = IntentSchema.call(preprocessed)
      raise ArgumentError, "Invalid intent: #{result.errors.to_h}" unless result.success?

      validated = result.to_h

      original_metrics = intent_hash['_metrics'] || intent_hash[:_metrics]
      validated['_metrics'] = original_metrics if original_metrics.is_a?(Hash)

      Array(validated['filters']).each_with_index do |f, idx|
        op = f['op'].to_s
        if %w[exists not_exists].include?(op)
          unless f['related_table'].to_s.strip != '' && f['fk_column'].to_s.strip != ''
            raise ArgumentError, "Invalid intent: filters[#{idx}] requires related_table and fk_column for #{op}"
          end

          f['base_column'] ||= 'id'
          f['column'] ||= 'id'
        else
          unless f['column'].to_s.strip != ''
            raise ArgumentError, "Invalid intent: filters[#{idx}].column must be filled"
          end
        end
      end
      enforce_allowlists!(validated, current_user: current_user, allow_tables: allow_tables)
      validated
    end

    private

    def preprocess_exists_filters(intent_hash)
      intent = intent_hash.dup

      if intent['filters'].is_a?(Array)
        intent['filters'] = intent['filters'].map do |filter|
          if filter.is_a?(Hash) && %w[exists not_exists].include?(filter['op'].to_s) && filter['column'].nil?
            filter.merge('column' => 'id')
          else
            filter
          end
        end
      end

      intent
    end

    # rubocop:disable Metrics/PerceivedComplexity
    def enforce_allowlists!(intent, current_user:, allow_tables:)
      # Enforce table allowlist if provided (from user input)
      if Array(allow_tables).any?
        table = intent['table']
        if (table.to_s.strip != '') && !Array(allow_tables).map { |t| t.to_s.downcase }.include?(table.to_s.downcase)
          raise ArgumentError, "Invalid intent: table '#{table}' not allowed"
        end
      end

      # Consult policy adapter for additional table/column allowlists
      adapter = CodeToQuery.config.policy_adapter
      return unless adapter.respond_to?(:call)

      policy_info = safe_call_policy_adapter(adapter, current_user, table: intent['table'], intent: intent)
      allowed_tables = Array(policy_info[:allowed_tables] || policy_info['allowed_tables']).map { |t| t.to_s.downcase }
      if allowed_tables.any?
        table = intent['table']
        if (table.to_s.strip != '') && !allowed_tables.include?(table.to_s.downcase)
          raise ArgumentError, "Invalid intent: table '#{table}' not permitted by policy"
        end
      end

      allowed_columns = policy_info[:allowed_columns] || policy_info['allowed_columns'] || {}
      return if allowed_columns.nil? || allowed_columns.empty?

      # Normalize map keys to strings with lowercase table and column names
      normalized = {}
      allowed_columns.each do |tbl, cols|
        normalized[tbl.to_s.downcase] = Array(cols).map { |c| c.to_s.downcase }
      end

      main_table = intent['table'].to_s.downcase

      # Columns in SELECT
      Array(intent['columns']).each do |col|
        next if col == '*'
        next unless normalized[main_table]&.any?
        unless normalized[main_table].include?(col.to_s.downcase)
          raise ArgumentError, "Invalid intent: selecting column '#{col}' not permitted on '#{main_table}'"
        end
      end

      # ORDER BY columns
      Array(intent['order']).each do |o|
        col = o['column']
        next if col.nil?
        next unless normalized[main_table]&.any?
        unless normalized[main_table].include?(col.to_s.downcase)
          raise ArgumentError, "Invalid intent: ordering by column '#{col}' not permitted on '#{main_table}'"
        end
      end

      # DISTINCT ON columns
      Array(intent['distinct_on']).each do |col|
        next unless normalized[main_table]&.any?
        unless normalized[main_table].include?(col.to_s.downcase)
          raise ArgumentError, "Invalid intent: distinct_on column '#{col}' not permitted on '#{main_table}'"
        end
      end

      # GROUP BY
      Array(intent['group_by']).each do |col|
        next unless normalized[main_table]&.any?
        unless normalized[main_table].include?(col.to_s.downcase)
          raise ArgumentError, "Invalid intent: group_by column '#{col}' not permitted on '#{main_table}'"
        end
      end

      # WHERE filters
      Array(intent['filters']).each do |f|
        op = f['op'].to_s
        if %w[exists not_exists].include?(op)
          related_table = f['related_table']
          rel_cols = normalized[related_table.to_s.downcase]
          next if rel_cols.nil? || rel_cols.empty?

          Array(f['related_filters']).each do |rf|
            col = rf['column']
            next if col.nil?
            unless rel_cols.include?(col.to_s.downcase)
              raise ArgumentError, "Invalid intent: filter column '#{col}' not permitted on '#{related_table}'"
            end
          end
        else
          col = f['column']
          next if col.nil?

          cols = normalized[main_table]
          next if cols.nil? || cols.empty?
          unless cols.include?(col.to_s.downcase)
            raise ArgumentError, "Invalid intent: filter column '#{col}' not permitted on '#{main_table}'"
          end
        end
      end
    rescue StandardError => e
      # Re-raise as ArgumentError to keep validator contract
      raise ArgumentError, e.message
    end
    # rubocop:enable Metrics/PerceivedComplexity

    def safe_call_policy_adapter(adapter, current_user, table:, intent:)
      adapter.call(current_user, table: table, intent: intent)
    rescue ArgumentError
      adapter.call(current_user, table: table)
    rescue StandardError
      {}
    end
  end
end
