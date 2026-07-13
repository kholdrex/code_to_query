# frozen_string_literal: true

require 'spec_helper'

RSpec.describe 'policy table metadata casing pipeline' do
  let(:config) { stub_config(adapter: :postgres, default_limit: 100) }
  let(:validator) { CodeToQuery::Validator.new }
  let(:compiler) { CodeToQuery::Compiler.new(config) }
  let(:policy_tables) { %w[AuditEvents auditevents] }

  before do
    config.policy_adapter = ->(_user, **) { { allowed_tables: policy_tables } }
  end

  after do
    config.policy_adapter = nil
  end

  def query_from_policy(table)
    validated = validator.validate(
      { 'type' => 'select', 'table' => table, 'columns' => ['*'] }
    ).deep_stringify_keys
    compiled = compiler.compile(validated)
    query = CodeToQuery::Query.new(
      sql: compiled[:sql],
      params: compiled[:params],
      bind_spec: compiled[:bind_spec],
      intent: compiled[:intent],
      allow_tables: nil,
      config: config,
      policy_contract: compiled[:policy_contract]
    )

    [validated, compiled, query]
  end

  it 'preserves a deliberately cased quoted PostgreSQL table from policy through query safety' do
    validated, compiled, query = query_from_policy('AuditEvents')

    expect(validated['__policy_allowed_tables']).to eq(policy_tables)
    expect(compiled[:intent]['__policy_allowed_tables']).to eq(policy_tables)
    expect(compiled[:sql]).to include('FROM "AuditEvents"')
    expect(query.safe?).to be true
  end

  it 'keeps a distinct lowercase PostgreSQL table authorized independently' do
    validated, compiled, query = query_from_policy('auditevents')

    expect(validated['__policy_allowed_tables']).to eq(policy_tables)
    expect(compiled[:intent]['__policy_allowed_tables']).to eq(policy_tables)
    expect(compiled[:sql]).to include('FROM "auditevents"')
    expect(query.safe?).to be true
  end

  it 'does not authorize a lowercase related table from a deliberately cased policy entry' do
    config.policy_adapter = lambda do |_user, **kwargs|
      if kwargs[:table] == 'accounts'
        { allowed_tables: %w[accounts AuditEvents] }
      else
        { allowed_tables: ['AuditEvents'] }
      end
    end
    intent = {
      'type' => 'select', 'table' => 'accounts', 'columns' => ['*'],
      'filters' => [{
        'op' => 'exists', 'related_table' => 'auditevents',
        'fk_column' => 'account_id', 'base_column' => 'id'
      }]
    }
    validated = validator.validate(intent).deep_stringify_keys

    expect { compiler.compile(validated) }
      .to raise_error(CodeToQuery::PolicyAdapterError, /does not allow related table: auditevents/)
  end
end
