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
  end

  describe '#safe?' do
    context 'with valid query' do
      it 'returns true for safe queries' do
        expect(query.safe?).to be true
      end
    end

    context 'with unsafe query' do
      let(:sql) { 'DROP TABLE users' }

      it 'returns false for unsafe queries' do
        expect(query.safe?).to be false
      end
    end

    it 'caches the safety check result' do
      linter = instance_double(CodeToQuery::Guardrails::SqlLinter)
      allow(CodeToQuery::Guardrails::SqlLinter).to receive(:new).and_return(linter)
      allow(linter).to receive(:check!).and_return(true)

      query.safe?
      query.safe?

      expect(linter).to have_received(:check!).once
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
      it 'uses PostgreSQL EXPLAIN format' do
        skip 'ActiveRecord mocking too complex for unit tests'
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
    it 'delegates to Runner' do
      runner = instance_double(CodeToQuery::Runner)
      allow(CodeToQuery::Runner).to receive(:new).with(config).and_return(runner)
      allow(runner).to receive(:run).and_return(double)

      query.run

      expect(runner).to have_received(:run).with(sql: sql, binds: [])
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
      it 'builds QueryAttribute objects from bind_spec' do
        skip 'ActiveRecord integration requires full Rails environment'
      end
    end
  end
end
