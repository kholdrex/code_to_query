# frozen_string_literal: true

# Configuration management

require 'singleton'
require 'logger'

module CodeToQuery
  # Centralized configuration with sensible defaults
  class Configuration
    include Singleton

    attr_accessor :adapter, :readonly_role, :default_limit, :max_limit, :max_joins,
                  :block_subqueries, :allow_seq_scans, :max_query_cost, :max_query_rows,
                  :query_timeout, :force_readonly_session, :reset_session_after_query,
                  :policy_adapter, :context_pack_path, :enable_explain_gate, :provider,
                  :openai_api_key, :openai_model, :stub_llm,
                  :auto_glossary_with_llm, :max_glossary_suggestions, :count_limit,
                  :aggregation_limit, :distinct_limit, :exists_limit,
                  :planner_max_attempts, :planner_feedback_mode, :prefer_static_scan,
                  :static_scan_dirs, :context_rag_top_k, :require_limit_by_default,
                  :explain_fail_open

    # Extended configuration knobs (added for LLM transport and logging)
    attr_accessor :logger, :llm_api_base, :llm_timeout, :llm_temperature, :provider_options, :system_prompt_template, :llm_client

    def initialize
      @adapter = :postgres
      @readonly_role = nil
      @default_limit = 100
      @max_limit = 10_000
      @max_joins = 3
      @block_subqueries = false
      @allow_seq_scans = false
      @max_query_cost = 10_000
      @max_query_rows = 100_000
      @query_timeout = 30
      @force_readonly_session = false
      @reset_session_after_query = false
      @policy_adapter = nil
      @context_pack_path = if defined?(Rails)
                             Rails.root.join('db/code_to_query/context.json')
                           else
                             File.join(Dir.pwd, 'db/code_to_query/context.json')
                           end
      @enable_explain_gate = false
      @explain_fail_open = true
      @provider = :auto
      @openai_api_key = ENV.fetch('OPENAI_API_KEY', nil)
      @openai_model = 'gpt-4'
      @stub_llm = false
      # LLM-assisted glossary enrichment during bootstrap (on by default for better UX)
      @auto_glossary_with_llm = true
      @max_glossary_suggestions = 200
      # Query type specific limits for flexibility
      @count_limit = nil          # No limit for COUNT operations by default
      @aggregation_limit = nil    # No limit for SUM/AVG/MAX/MIN operations by default
      @distinct_limit = 10_000    # Higher limit for DISTINCT queries
      @exists_limit = 1           # LIMIT 1 for existence checks

      # Planner iteration
      @planner_max_attempts = Integer(ENV.fetch('CODE_TO_QUERY_PLANNER_MAX_ATTEMPTS', 2))
      # feedback modes: :none, :schema_strict, :adaptive
      @planner_feedback_mode = ENV.fetch('CODE_TO_QUERY_PLANNER_FEEDBACK_MODE', 'adaptive').to_sym

      # Logging and LLM provider knobs
      @logger = if defined?(Rails) && Rails.respond_to?(:logger)
                  Rails.logger
                else
                  Logger.new($stdout).tap { |l| l.level = Logger::WARN }
                end
      @llm_api_base = ENV.fetch('CODE_TO_QUERY_LLM_API_BASE', 'https://api.openai.com/v1')
      @llm_timeout = Integer(ENV.fetch('CODE_TO_QUERY_LLM_TIMEOUT', 30))
      @llm_temperature = Float(ENV.fetch('CODE_TO_QUERY_LLM_TEMPERATURE', 0.1))
      @provider_options = {}
      @system_prompt_template = nil
      @llm_client = nil

      # Static analysis and RAG context options
      @prefer_static_scan = true
      @static_scan_dirs = if defined?(Rails)
                            [Rails.root.join('app/models').to_s]
                          else
                            [File.join(Dir.pwd, 'app/models')]
                          end
      @context_rag_top_k = Integer(ENV.fetch('CODE_TO_QUERY_CONTEXT_RAG_TOP_K', 6))

      # Guardrail defaults
      @require_limit_by_default = true
    end
  end
end
