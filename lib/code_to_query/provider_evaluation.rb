# frozen_string_literal: true

require 'yaml'
require 'code_to_query'

module CodeToQuery
  class ProviderEvaluation
    DEFAULT_FIXTURE_PATH = File.expand_path('provider_evaluation/local.yml', __dir__)
    REQUIRED_CASE_KEYS = %w[name prompt allow_tables expect].freeze

    Result = Struct.new(:case_name, :passed, :failures, :intent, keyword_init: true)

    attr_reader :cases, :results

    def self.load_file(path = DEFAULT_FIXTURE_PATH, config: CodeToQuery.config)
      data = YAML.safe_load_file(path, permitted_classes: [], aliases: false)
      new(data, config: config)
    end

    def initialize(data, config: CodeToQuery.config)
      @data = data || {}
      @config = config
      @cases = Array(@data.fetch('cases', []))
      @results = []
      validate_cases!
    end

    def run(provider: :local)
      with_provider(provider) do
        @results = cases.map { |evaluation_case| evaluate_case(evaluation_case) }
      end

      self
    end

    def success_rate
      return 0.0 if results.empty?

      results.count(&:passed).fdiv(results.length)
    end

    def passed?
      results.any? && results.all?(&:passed)
    end

    def summary
      {
        total: results.length,
        passed: results.count(&:passed),
        failed: results.count { |result| !result.passed },
        success_rate: success_rate
      }
    end

    private

    def validate_cases!
      raise ArgumentError, 'provider evaluation requires at least one case' if cases.empty?

      cases.each_with_index do |evaluation_case, index|
        missing = REQUIRED_CASE_KEYS.reject { |key| evaluation_case.key?(key) }
        raise ArgumentError, "provider evaluation case #{index} is missing: #{missing.join(', ')}" unless missing.empty?
        raise ArgumentError, "provider evaluation case #{index} expect must be a Hash" unless evaluation_case['expect'].is_a?(Hash)
      end
    end

    def with_provider(provider)
      # Evaluation mutates the shared config for a single rake/spec process; it is not thread-safe.
      original_provider = @config.provider
      @config.provider = provider
      yield
    ensure
      @config.provider = original_provider
    end

    def evaluate_case(evaluation_case)
      intent = Planner.new(@config).plan(
        prompt: evaluation_case.fetch('prompt'),
        schema: schema,
        allow_tables: evaluation_case.fetch('allow_tables')
      )

      failures = expectation_failures(
        evaluation_case.fetch('expect'),
        intent,
        evaluation_case.fetch('allow_tables')
      )
      Result.new(
        case_name: evaluation_case.fetch('name'),
        passed: failures.empty?,
        failures: failures,
        intent: intent
      )
    end

    def schema
      @data.fetch('schema', {})
    end

    def expectation_failures(expectations, intent, allow_tables)
      failures = []
      failures << 'intent must be a Hash' unless intent.is_a?(Hash)
      return failures unless intent.is_a?(Hash)

      failures.concat(table_failures(expectations, intent, allow_tables))
      failures.concat(limit_failures(expectations, intent))
      failures.concat(limit_ceiling_failures(expectations, intent))
      failures.concat(params_failures(expectations, intent))
      failures.concat(metrics_failures(expectations, intent))
      failures
    end

    def table_failures(expectations, intent, allow_tables)
      expected_table = expectations['table']
      if expected_table
        return [] if intent['table'].to_s == expected_table.to_s

        return ["expected table #{expected_table.inspect}, got #{intent['table'].inspect}"]
      end

      return [] unless expectations['table_in_allowlist']
      return [] if Array(allow_tables).map(&:to_s).include?(intent['table'].to_s)

      ["expected table to be in allowlist #{allow_tables.inspect}, got #{intent['table'].inspect}"]
    end

    def limit_failures(expectations, intent)
      return [] unless expectations.key?('limit')
      return [] if intent['limit'] == expectations['limit']

      ["expected limit #{expectations['limit'].inspect}, got #{intent['limit'].inspect}"]
    end

    def limit_ceiling_failures(expectations, intent)
      return [] unless expectations.key?('limit_at_most')

      limit = intent['limit']
      unless limit.is_a?(Integer) && limit.positive?
        return ["expected positive bounded limit at most #{expectations['limit_at_most'].inspect}, got #{limit.inspect}"]
      end
      return [] if limit <= expectations['limit_at_most'].to_i

      ["expected limit at most #{expectations['limit_at_most'].inspect}, got #{limit.inspect}"]
    end

    def params_failures(expectations, intent)
      return [] unless expectations['params_hash']
      return [] if intent['params'].is_a?(Hash)

      ["expected params to be a Hash, got #{intent['params'].class}"]
    end

    def metrics_failures(expectations, intent)
      return [] unless expectations['provider_metrics']
      return [] if intent['_metrics'].is_a?(Hash) && (intent['_metrics'].key?(:elapsed_s) || intent['_metrics'].key?('elapsed_s'))

      ['expected provider metrics; planner may have returned fallback intent']
    end
  end
end
