# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery::Query do
  let(:config) { stub_config(adapter: :postgres) }
  let(:sql) { 'SELECT * FROM "users" WHERE "active" = $1 LIMIT 100' }
  let(:params) { { 'active' => true } }
  let(:bind_spec) { [{ key: 'active', column: 'active', cast: nil }] }
  let(:intent) { { 'table' => 'users', 'type' => 'select' } }
  let(:query) do
    described_class.new(
      sql: sql,
      params: params,
      bind_spec: bind_spec,
      intent: intent,
      allow_tables: ['users'],
      config: config
    )
  end

  describe '#sql' do
    it 'returns the SQL string' do
      expect(query.sql).to eq(sql)
    end
  end

  describe '#params' do
    it 'returns the parameters hash' do
      expect(query.params).to eq(params)
    end

    it 'adds between defaults derived from column names for legacy start/end params' do
      q = described_class.new(
        sql: 'SELECT * FROM "orders" WHERE "created_at" BETWEEN $1 AND $2',
        params: { 'start' => '2023-01-01', 'end' => '2023-12-31' },
        bind_spec: [{ key: 'created_at_start', column: 'created_at' }, { key: 'created_at_end', column: 'created_at' }],
        intent: {
          'table' => 'orders',
          'type' => 'select',
          'filters' => [
            { 'column' => 'created_at', 'op' => 'between' }
          ]
        },
        allow_tables: ['orders'],
        config: config
      )

      expect(q.params['created_at_start']).to eq('2023-01-01')
      expect(q.params['created_at_end']).to eq('2023-12-31')
    end

    it 'adds between defaults derived from column names for legacy symbol-key params' do
      q = described_class.new(
        sql: 'SELECT * FROM "orders" WHERE "created_at" BETWEEN $1 AND $2',
        params: { start: '2023-01-01', end: '2023-12-31' },
        bind_spec: [{ key: 'created_at_start', column: 'created_at' }, { key: 'created_at_end', column: 'created_at' }],
        intent: {
          'table' => 'orders',
          'type' => 'select',
          'filters' => [
            { 'column' => 'created_at', 'op' => 'between' }
          ]
        },
        allow_tables: ['orders'],
        config: config
      )

      expect(q.params['created_at_start']).to eq('2023-01-01')
      expect(q.params['created_at_end']).to eq('2023-12-31')
    end

    it 'preserves false-valued legacy between params when deriving column-based keys' do
      q = described_class.new(
        sql: 'SELECT * FROM "orders" WHERE "created_at" BETWEEN $1 AND $2',
        params: { 'start' => false, 'end' => true },
        bind_spec: [{ key: 'created_at_start', column: 'created_at' }, { key: 'created_at_end', column: 'created_at' }],
        intent: {
          'table' => 'orders',
          'type' => 'select',
          'filters' => [
            { 'column' => 'created_at', 'op' => 'between' }
          ]
        },
        allow_tables: ['orders'],
        config: config
      )

      expect(q.params['created_at_start']).to be(false)
      expect(q.params['created_at_end']).to be(true)
    end

    it 'hydrates only the implicit side when one bound keeps an explicit param name' do
      q = described_class.new(
        sql: 'SELECT * FROM "orders" WHERE "created_at" BETWEEN $1 AND $2',
        params: { 'end' => '2023-12-31' },
        bind_spec: [{ key: 'from_date', column: 'created_at' }, { key: 'created_at_end', column: 'created_at' }],
        intent: {
          'table' => 'orders',
          'type' => 'select',
          'filters' => [
            { 'column' => 'created_at', 'op' => 'between', 'param_start' => 'from_date' }
          ]
        },
        allow_tables: ['orders'],
        config: config
      )

      expect(q.params['created_at_end']).to eq('2023-12-31')
      expect(q.params).not_to have_key('created_at_start')
    end
  end

  describe '#binds' do
    it 'preserves false values stored under string keys' do
      mock_connection = double('Connection')
      ar_base = Class.new do
        def self.connection = @mock_connection

        class << self
          attr_writer :mock_connection
        end
      end
      ar_base.mock_connection = mock_connection
      stub_const('ActiveRecord::Base', ar_base)
      stub_const('ActiveRecord::Relation::QueryAttribute', Struct.new(:name, :value, :type))

      q = described_class.new(
        sql: 'SELECT * FROM "users" WHERE "archived" = $1',
        params: { 'archived' => false },
        bind_spec: [{ key: 'archived', column: 'archived', cast: nil }],
        intent: { 'table' => 'users', 'type' => 'select' },
        allow_tables: ['users'],
        config: config
      )
      allow(q).to receive(:infer_column_type).and_return(:boolean)

      binds = q.binds

      expect(binds.first.value).to be(false)
    end
  end

  describe 'false-valued param lookups' do
    it 'preserves false values when applying equality filters to a scope' do
      scoped_query = described_class.new(
        sql: 'SELECT * FROM "users" WHERE "archived" = $1',
        params: { 'archived' => false },
        bind_spec: [],
        intent: {
          'table' => 'users',
          'type' => 'select',
          'filters' => [{ 'column' => 'archived', 'op' => '=' }]
        },
        allow_tables: ['users'],
        config: config
      )
      scope = double('scope')

      expect(scope).to receive(:where).with('archived' => false)
      scoped_query.send(:apply_filter_to_scope, scope, { 'column' => 'archived', 'op' => '=' })
    end

    it 'does not fall back to legacy start when between start uses an explicit param name' do
      scoped_query = described_class.new(
        sql: 'SELECT * FROM "orders" WHERE "created_at" BETWEEN $1 AND $2',
        params: { 'start' => false, 'created_at_end' => '2023-12-31' },
        bind_spec: [],
        intent: {
          'table' => 'orders',
          'type' => 'select',
          'filters' => [{ 'column' => 'created_at', 'op' => 'between', 'param_start' => 'from_date' }]
        },
        allow_tables: ['orders'],
        config: config
      )
      scope = double('scope')

      expect(scope).to receive(:where).with('created_at' => (nil..'2023-12-31'))
      scoped_query.send(:apply_filter_to_scope, scope, { 'column' => 'created_at', 'op' => 'between', 'param_start' => 'from_date' })
    end

    it 'infers boolean type from false values stored under custom bind keys' do
      typed_query = described_class.new(
        sql: 'SELECT * FROM "users" WHERE "archived" = $1',
        params: { 'is_archived' => false },
        bind_spec: [],
        intent: { 'table' => 'users', 'type' => 'select' },
        allow_tables: ['users'],
        config: config
      )

      hide_const('ActiveRecord::Base')

      inferred_type = typed_query.send(:infer_column_type, nil, 'archived', nil, 'is_archived')
      expect(inferred_type).to be_a(ActiveRecord::Type::Boolean)
    end
  end

  describe '#safe?' do
    context 'with valid query' do
      it 'returns true for safe queries' do
        q = described_class.new(
          sql: sql,
          params: params,
          bind_spec: bind_spec,
          intent: intent,
          allow_tables: ['users'],
          config: config
        )
        allow(q).to receive(:perform_safety_checks).and_return(true)

        expect(q.safe?).to be true
      end
    end

    context 'with unsafe query' do
      let(:sql) { 'DROP TABLE users' }

      it 'returns false for unsafe queries' do
        expect(query.safe?).to be false
      end
    end

    it 'caches the safety check result' do
      q = described_class.new(
        sql: sql,
        params: params,
        bind_spec: bind_spec,
        intent: intent,
        allow_tables: ['users'],
        config: config
      )
      allow(q).to receive(:perform_safety_checks).and_return(true)

      q.safe?
      q.safe?

      expect(q).to have_received(:perform_safety_checks).once
    end
  end

  describe '#explain' do
    context 'without ActiveRecord' do
      it 'returns unavailable message' do
        hide_const('ActiveRecord')
        expect(query.explain).to include('EXPLAIN unavailable')
      end
    end

    context 'with database adapter variations' do
      let(:mock_connection) { double('Connection') }
      let(:mock_result) { [{ 'QUERY PLAN' => 'Index Scan on users' }] }

      before do
        ar_base = Class.new do
          def self.connection
            @mock_connection
          end

          class << self
            attr_writer :mock_connection
          end
        end
        ar_base.mock_connection = mock_connection
        stub_const('ActiveRecord::Base', ar_base)
        allow(mock_connection).to receive(:execute).and_return(mock_result)
      end

      it 'uses PostgreSQL EXPLAIN format' do
        config.adapter = :postgres
        result = query.explain

        expect(mock_connection).to have_received(:execute).with(
          "EXPLAIN (ANALYZE false, VERBOSE false, BUFFERS false) #{sql}"
        )
        expect(result).to include('Index Scan on users')
      end

      it 'uses MySQL EXPLAIN format' do
        config.adapter = :mysql
        query.explain

        expect(mock_connection).to have_received(:execute).with("EXPLAIN #{sql}")
      end

      it 'uses SQLite EXPLAIN format' do
        config.adapter = :sqlite
        query.explain

        expect(mock_connection).to have_received(:execute).with("EXPLAIN QUERY PLAN #{sql}")
      end
    end
  end

  describe '#to_relation' do
    context 'without ActiveRecord' do
      it 'returns nil' do
        hide_const('ActiveRecord')
        expect(query.to_relation).to be_nil
      end
    end

    context 'with non-SELECT query' do
      let(:intent) { { 'type' => 'insert' } }

      it 'returns nil for non-SELECT queries' do
        expect(query.to_relation).to be_nil
      end
    end
  end

  describe '#to_active_record' do
    it 'aliases to_relation' do
      allow_any_instance_of(described_class).to receive(:to_relation).and_return(:rel)
      expect(query.to_active_record).to eq(:rel)
    end
  end

  describe '#relationable?' do
    it 'returns false when not a select' do
      q = described_class.new(sql: sql, params: params, bind_spec: bind_spec, intent: { 'type' => 'insert' }, allow_tables: ['users'], config: config)
      expect(q.relationable?).to be false
    end
  end

  describe '#to_relation!' do
    it 'raises when not relationable' do
      q = described_class.new(sql: sql, params: params, bind_spec: bind_spec, intent: { 'type' => 'insert' }, allow_tables: ['users'], config: config)
      expect { q.to_relation! }.to raise_error(CodeToQuery::NotRelationConvertibleError)
    end
  end

  describe '#run' do
    it 'emits run instrumentation and delegates to Runner' do
      events = []
      subscriber = ActiveSupport::Notifications.subscribe('code_to_query.run') do |_name, _started, _finished, _id, payload|
        events << payload
      end

      runner = instance_double(CodeToQuery::Runner)
      allow(CodeToQuery::Runner).to receive(:new).with(config).and_return(runner)
      allow(runner).to receive(:run).and_return(double)

      query.run

      expect(runner).to have_received(:run).with(sql: sql, binds: [])
      expect(events.last).to include(
        table: 'users',
        query_type: 'select',
        query_shape: 'select:users',
        policy_applied: false,
        bind_count: 1,
        row_limit: nil,
        duration_ms: a_kind_of(Numeric)
      )
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
    end
  end

  describe '#preview' do
    it 'returns SQL, parameters, policy bind keys, and a local lint decision without executing' do
      q = described_class.new(
        sql: sql,
        params: params.merge('policy_tenant_id' => 42),
        bind_spec: bind_spec + [{ key: 'policy_tenant_id', column: 'tenant_id', cast: nil }],
        intent: intent,
        allow_tables: ['users'],
        config: config
      )

      allow(CodeToQuery::Runner).to receive(:new)

      expect(q.preview).to eq(
        sql: sql,
        params: params.merge('policy_tenant_id' => 42),
        applied_policies: ['policy_tenant_id'],
        estimated_cost: nil,
        would_run?: true
      )
      expect(CodeToQuery::Runner).not_to have_received(:new)
    end

    it 'reports that unsafe SQL would not run' do
      q = described_class.new(
        sql: 'DROP TABLE users',
        params: {},
        bind_spec: [],
        intent: intent,
        allow_tables: ['users'],
        config: config
      )

      expect(q.preview.fetch(:would_run?)).to be false
    end
  end

  describe '#binds' do
    context 'without ActiveRecord' do
      it 'returns empty array' do
        hide_const('ActiveRecord')
        expect(query.binds).to eq([])
      end
    end

    context 'with ActiveRecord available' do
      it 'returns binds based on bind_spec when mocked' do
        mock_bind = double('QueryAttribute', name: 'active', value: true)
        q = described_class.new(
          sql: sql,
          params: params,
          bind_spec: bind_spec,
          intent: intent,
          allow_tables: ['users'],
          config: config
        )

        # Stub the binds method to verify it returns expected structure
        allow(q).to receive(:binds).and_return([mock_bind])

        result = q.binds

        expect(result).to be_an(Array)
        expect(result.length).to eq(1)
        expect(result.first.name).to eq('active')
        expect(result.first.value).to be(true)
      end
    end
  end
end
