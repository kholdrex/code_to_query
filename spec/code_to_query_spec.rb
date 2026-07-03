# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery do
  describe '.configure' do
    it 'yields the configuration instance' do
      expect { |b| described_class.configure(&b) }.to yield_with_args(CodeToQuery::Configuration.instance)
    end

    it 'allows setting configuration options' do
      described_class.configure do |config|
        config.default_limit = 200
        config.adapter = :mysql
      end

      expect(described_class.config.default_limit).to eq(200)
      expect(described_class.config.adapter).to eq(:mysql)
    end
  end

  describe '.ask' do
    before do
      stub_config(stub_llm: true, provider: :local)
    end

    let(:exists_related_filter) do
      {
        'column' => 'id',
        'op' => 'exists',
        'related_table' => 'answers',
        'fk_column' => 'question_id',
        'base_column' => 'id',
        'related_filters' => []
      }
    end

    def stub_ask_pipeline(compiled_intent:, allow_tables:, sql:)
      planner = instance_double(CodeToQuery::Planner)
      validator = instance_double(CodeToQuery::Validator)
      compiler = instance_double(CodeToQuery::Compiler)
      linter = instance_double(CodeToQuery::Guardrails::SqlLinter)

      allow(CodeToQuery::Planner).to receive(:new).and_return(planner)
      allow(CodeToQuery::Validator).to receive(:new).and_return(validator)
      allow(CodeToQuery::Compiler).to receive(:new).and_return(compiler)
      allow(CodeToQuery::Guardrails::SqlLinter).to receive(:new)
        .with(described_class.config, allow_tables: allow_tables)
        .and_return(linter)

      allow(planner).to receive(:plan).and_return(compiled_intent)
      allow(validator).to receive(:validate).and_return(compiled_intent)
      allow(compiler).to receive(:compile).and_return(
        sql: sql,
        params: {},
        bind_spec: [],
        intent: compiled_intent
      )
      allow(linter).to receive(:check!)
    end

    # rubocop:disable RSpec/MultipleExpectations,RSpec/ExampleLength
    it 'emits non-sensitive pipeline instrumentation' do
      events = []
      subscriber = ActiveSupport::Notifications.subscribe(/\Acode_to_query\./) do |name, started, finished, _id, payload|
        events << [name, payload, started, finished]
      end

      described_class.ask(prompt: 'Get users', allow_tables: ['users'])

      event_names = events.map(&:first)
      expect(event_names).to include(
        'code_to_query.plan',
        'code_to_query.validate',
        'code_to_query.compile',
        'code_to_query.lint'
      )
      expect(events.map { |event| event[1] }).not_to include(include(prompt: 'Get users'))
      expect(events.map { |event| event[1] }).not_to include(include(:sql))
      expect(events.map { |event| event[1] }).not_to include(include(:params))
      expect(events.map { |event| event[1] }).to all(include(duration_ms: a_kind_of(Numeric)))
      validate_payload = events.find { |name, _payload, _started, _finished| name == 'code_to_query.validate' }&.at(1) || {}
      compile_payload = events.find { |name, _payload, _started, _finished| name == 'code_to_query.compile' }&.at(1) || {}
      lint_payload = events.find { |name, _payload, _started, _finished| name == 'code_to_query.lint' }&.at(1) || {}

      limit = validate_payload[:row_limit]

      expect(validate_payload).to include(query_shape: 'select:users')
      expect(compile_payload).to include(policy_applied: false, query_shape: 'select:users')
      expect(lint_payload).to include(policy_applied: false, query_shape: 'select:users')
      expect(limit).to be_a(Integer)
      expect(compile_payload[:row_limit]).to eq(limit)
      expect(lint_payload[:row_limit]).to eq(limit)
      expect(events.map { |event| event[1][:duration_ms] }).to all(be >= 0)
      expect(events.map { |event| event[1] }).to include(include(query_shape: include('users')))
      expect(events).to all(satisfy { |_name, _payload, started, finished| finished >= started })
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
    end
    # rubocop:enable RSpec/MultipleExpectations,RSpec/ExampleLength

    it 'returns a Query object' do
      query = described_class.ask(prompt: 'Get users', allow_tables: ['users'])
      expect(query).to be_a(CodeToQuery::Query)
    end

    it 'preserves compiler-augmented intent on the returned query' do
      planner = instance_double(CodeToQuery::Planner)
      validator = instance_double(CodeToQuery::Validator)
      compiler = instance_double(CodeToQuery::Compiler)
      linter = instance_double(CodeToQuery::Guardrails::SqlLinter)

      compiled_intent = sample_intent.merge(
        'filters' => [
          { 'column' => 'tenant_id', 'op' => '=', 'param' => 'policy_tenant_id' }
        ],
        'params' => { 'policy_tenant_id' => 42 }
      )

      allow(CodeToQuery::Planner).to receive(:new).and_return(planner)
      allow(CodeToQuery::Validator).to receive(:new).and_return(validator)
      allow(CodeToQuery::Compiler).to receive(:new).and_return(compiler)
      allow(CodeToQuery::Guardrails::SqlLinter).to receive(:new).and_return(linter)

      allow(planner).to receive(:plan).and_return(sample_intent)
      allow(validator).to receive(:validate).and_return(sample_intent)
      allow(compiler).to receive(:compile).and_return(
        sql: 'SELECT users.* FROM users WHERE users.tenant_id = $1',
        params: { 'policy_tenant_id' => 42 },
        bind_spec: [{ key: 'policy_tenant_id', column: 'tenant_id', cast: nil }],
        intent: compiled_intent
      )
      allow(linter).to receive(:check!)

      query = described_class.ask(prompt: 'Get users', allow_tables: ['users'])

      expect(query.intent).to eq(compiled_intent)
    end

    it 'rejects EXISTS related tables unless they are explicitly allowed' do
      compiled_intent = {
        'table' => 'questions',
        'type' => 'select',
        'filters' => [exists_related_filter]
      }

      stub_ask_pipeline(
        compiled_intent: compiled_intent,
        allow_tables: ['questions'],
        sql: 'SELECT * FROM "questions" WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."question_id" = "questions"."id")'
      )

      expect { described_class.ask(prompt: 'Get questions', allow_tables: ['questions']) }
        .to raise_error(SecurityError, /allowed list/i)
    end

    it 'does not invent a partial allowlist when ask is called without allow_tables' do
      compiled_intent = {
        'table' => 'questions',
        'type' => 'select',
        'filters' => [exists_related_filter]
      }

      stub_ask_pipeline(
        compiled_intent: compiled_intent,
        allow_tables: nil,
        sql: 'SELECT * FROM "questions" WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."question_id" = "questions"."id")'
      )

      expect { described_class.ask(prompt: 'Get questions') }.not_to raise_error
    end

    # rubocop:disable RSpec/ExampleLength
    it 'marks compile/lint telemetry as policy-applied when policy binds are present' do
      events = []
      subscriber = ActiveSupport::Notifications.subscribe(/\Acode_to_query\./) do |name, started, finished, _id, payload|
        events << [name, payload, started, finished]
      end

      planner = instance_double(CodeToQuery::Planner)
      validator = instance_double(CodeToQuery::Validator)
      compiler = instance_double(CodeToQuery::Compiler)
      linter = instance_double(CodeToQuery::Guardrails::SqlLinter)

      allow(CodeToQuery::Planner).to receive(:new).and_return(planner)
      allow(CodeToQuery::Validator).to receive(:new).and_return(validator)
      allow(CodeToQuery::Compiler).to receive(:new).and_return(compiler)
      allow(CodeToQuery::Guardrails::SqlLinter).to receive(:new).and_return(linter)

      allow(planner).to receive(:plan).and_return(sample_intent)
      allow(validator).to receive(:validate).and_return(sample_intent)
      allow(compiler).to receive(:compile).and_return(
        sql: 'SELECT users.* FROM users WHERE users.tenant_id = $1',
        params: { 'policy_tenant_id' => 42 },
        bind_spec: [
          {
            key: 'policy_tenant_id',
            column: 'tenant_id',
            cast: nil
          }
        ]
      )
      allow(linter).to receive(:check!)

      described_class.ask(prompt: 'Get users', allow_tables: ['users'])

      compile_payload = events.find { |name, _payload, _started, _finished| name == 'code_to_query.compile' }&.at(1) || {}
      lint_payload = events.find { |name, _payload, _started, _finished| name == 'code_to_query.lint' }&.at(1) || {}

      expect(compile_payload).to include(policy_applied: true, query_shape: 'select:users')
      expect(lint_payload).to include(policy_applied: true, query_shape: 'select:users')
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
    end
    # rubocop:enable RSpec/ExampleLength

    it 'passes parameters to the planner' do
      planner = instance_double(CodeToQuery::Planner)
      allow(CodeToQuery::Planner).to receive(:new).and_return(planner)
      allow(planner).to receive(:plan).and_return(sample_intent)

      described_class.ask(prompt: 'Get users', schema: sample_schema, allow_tables: ['users'])

      expect(planner).to have_received(:plan).with(
        prompt: 'Get users',
        schema: sample_schema,
        allow_tables: ['users']
      )
    end
  end
end
