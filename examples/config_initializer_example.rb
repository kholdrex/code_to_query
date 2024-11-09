# frozen_string_literal: true

# Example configuration for CodeToQuery
# Place this in config/initializers/code_to_query.rb

CodeToQuery.configure do |config|
  # Database settings
  config.adapter = :postgres
  config.readonly_role = :reporting
  config.default_limit = 100
  config.max_limit = 10000
  config.max_joins = 3
  
  # OpenAI settings
  config.openai_api_key = ENV['OPENAI_API_KEY']
  config.openai_model = 'gpt-4'
  config.stub_llm = false
  
  # Security settings
  config.enable_explain_gate = true
  config.allow_seq_scans = false
  config.max_query_cost = 10000
  config.max_query_rows = 100000
  config.block_subqueries = false
  config.force_readonly_session = true
  
  # Query execution
  config.query_timeout = 30
  config.reset_session_after_query = false
  
  # Context pack path
  config.context_pack_path = Rails.root.join("db/code_to_query/context.json")
  
  # Policy enforcement
  config.policy_adapter = ->(user) do
    return {} unless user
    
    policies = {}
    
    if user.respond_to?(:tenant_id)
      policies[:tenant_id] = user.tenant_id
    end
    
    if user.respond_to?(:company_id)
      policies[:company_id] = user.company_id
    end
    
    unless user.admin?
      policies[:user_id] = user.id if user.respond_to?(:id)
    end
    
    policies
  end
end

# Development/Test overrides
if Rails.env.development? || Rails.env.test?
  CodeToQuery.configure do |config|
    config.allow_seq_scans = true
    config.max_query_cost = 50000
    config.enable_explain_gate = false
    config.stub_llm = true if Rails.env.test?
  end
end

# Production settings
if Rails.env.production?
  CodeToQuery.configure do |config|
    config.default_limit = 50
    config.max_limit = 1000
    config.max_joins = 2
    config.query_timeout = 15
    config.force_readonly_session = true
    config.block_subqueries = true
    config.enable_explain_gate = true
    
    unless config.openai_api_key.present?
      Rails.logger.warn "CodeToQuery: OpenAI API key not configured"
    end
    
    unless config.readonly_role
      Rails.logger.warn "CodeToQuery: Readonly database role not configured"
    end
  end
end
