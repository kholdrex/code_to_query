# frozen_string_literal: true

# Main entry point for CodeToQuery gem

require 'logger'
require 'active_support'
require 'active_support/core_ext/object/blank'
require 'json'
require 'active_support/core_ext/hash/keys'

require_relative 'code_to_query/version'
require_relative 'code_to_query/configuration'
require_relative 'code_to_query/errors'
require_relative 'code_to_query/instrumentation'
require_relative 'code_to_query/policy_adapter_invoker'
require_relative 'code_to_query/providers/base'
require_relative 'code_to_query/providers/openai'
require_relative 'code_to_query/providers/local'
require_relative 'code_to_query/planner'
require_relative 'code_to_query/validator'
require_relative 'code_to_query/compiler'
require_relative 'code_to_query/runner'
require_relative 'code_to_query/result_export'
require_relative 'code_to_query/query'
require_relative 'code_to_query/guardrails/sql_linter'
require_relative 'code_to_query/guardrails/explain_gate'
require_relative 'code_to_query/context/pack'
require_relative 'code_to_query/context/builder'
require_relative 'code_to_query/performance/cache'
require_relative 'code_to_query/performance/optimizer'
require_relative 'code_to_query/llm_client'
require_relative 'code_to_query/policies/pundit_adapter'
require_relative 'code_to_query/railtie' if defined?(Rails)

module CodeToQuery
  class Error < StandardError; end

  # Backward compatibility for new configuration accessors in older environments/tests
  module BackCompat
    module_function

    def ensure_extended_config!(config)
      # Logger
      unless config.respond_to?(:logger)
        class << config
          attr_accessor :logger
        end
        config.logger = if defined?(Rails) && Rails.respond_to?(:logger)
                          Rails.logger
                        else
                          Logger.new($stdout)
                        end
      end

      # LLM knobs and prompt template
      unless config.respond_to?(:policy_adapter_fail_open)
        class << config
          attr_accessor :policy_adapter_fail_open
        end
        config.policy_adapter_fail_open = false
      end

      return if config.respond_to?(:system_prompt_template)

      class << config
        attr_accessor :system_prompt_template, :llm_api_base, :llm_timeout, :llm_temperature, :provider_options
      end
      config.system_prompt_template = nil
      config.llm_api_base = ENV['CODE_TO_QUERY_LLM_API_BASE'] || 'https://api.openai.com/v1'
      config.llm_timeout = Integer(ENV['CODE_TO_QUERY_LLM_TIMEOUT'] || 30)
      config.llm_temperature = Float(ENV['CODE_TO_QUERY_LLM_TEMPERATURE'] || 0.1)
      config.provider_options = {}
    end
  end

  # Configure the gem
  def self.configure
    BackCompat.ensure_extended_config!(Configuration.instance)
    yield(Configuration.instance)
  end

  # Access the current configuration
  def self.config
    BackCompat.ensure_extended_config!(Configuration.instance)
    Configuration.instance
  end

  # Convert natural language to SQL query
  # current_user is optional and only used if a policy_adapter requires it
  def self.ask(prompt:, schema: nil, allow_tables: nil, current_user: nil)
    intent = Instrumentation.instrument(:plan, allow_tables: allow_tables) do
      Planner.new(config).plan(prompt: prompt, schema: schema, allow_tables: allow_tables)
    end

    validate_payload = {
      table: intent['table'] || intent[:table],
      query_type: intent['type'] || intent[:type],
      query_shape: query_shape(intent),
      row_limit: intent['limit'] || intent[:limit]
    }
    validated_intent = Instrumentation.instrument(:validate, **validate_payload) do
      Validator.new.validate(intent, current_user: current_user, allow_tables: allow_tables).deep_stringify_keys
    end

    compile_payload = {
      table: validated_intent['table'],
      query_type: validated_intent['type'],
      limit: validated_intent['limit'],
      query_shape: query_shape(validated_intent),
      row_limit: validated_intent['limit'],
      policy_applied: false
    }
    compiled = Instrumentation.instrument(:compile, compile_payload) do
      compile_result = Compiler.new(config).compile(validated_intent, current_user: current_user)
      compile_payload[:policy_applied] = policy_applied_from_bind_spec?(compile_result[:bind_spec])
      compile_result
    end

    policy_applied = compile_payload[:policy_applied]

    lint_payload = {
      table: validated_intent['table'],
      query_type: validated_intent['type'],
      query_shape: query_shape(validated_intent),
      row_limit: validated_intent['limit'],
      policy_applied: policy_applied
    }
    query = Query.new(sql: compiled[:sql], params: compiled[:params], bind_spec: compiled[:bind_spec],
                      intent: compiled[:intent] || validated_intent, allow_tables: allow_tables, config: config)

    begin
      Instrumentation.instrument(:lint, **lint_payload) do
        query.send(:lint_sql!)
      end
    rescue SecurityError => e
      Instrumentation.instrument(
        :lint_reject,
        table: validated_intent['table'],
        query_type: validated_intent['type'],
        row_limit: validated_intent['limit'],
        policy_applied: policy_applied,
        reason: e.class.name,
        query_shape: query_shape(validated_intent)
      )
      raise
    end

    query
  end

  def self.query_shape(intent)
    table = (intent || {})[:table] || (intent || {})['table']
    query_type = (intent || {})[:type] || (intent || {})['type']
    [query_type, table].compact.join(':')
  end

  def self.policy_applied_from_bind_spec?(bind_spec)
    Array(bind_spec).any? do |bind|
      key = bind.is_a?(Hash) ? (bind[:key] || bind['key']) : nil
      key.to_s.start_with?('policy_')
    end
  end

  private_class_method :query_shape
  private_class_method :policy_applied_from_bind_spec?
end
