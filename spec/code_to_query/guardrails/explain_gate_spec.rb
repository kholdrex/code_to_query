# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery::Guardrails::ExplainGate do
  let(:config) do
    stub_config(
      adapter: :postgres,
      max_query_cost: 10_000,
      max_query_rows: 100_000,
      allow_seq_scans: false,
      explain_fail_open: true
    )
  end
  let(:gate) { described_class.new(config) }

  describe '#allowed?' do
    context 'with default configuration' do
      let(:default_config) { CodeToQuery::Configuration.send(:new) }
      let(:default_gate) { described_class.new(default_config) }

      before do
        ar_base = Class.new do
          def self.connected?
            true
          end
        end
        stub_const('ActiveRecord::Base', ar_base)
        allow(default_gate).to receive(:get_explain_plan).and_raise(StandardError, 'connection error')
      end

      it 'fails closed when EXPLAIN raises' do
        expect(default_gate.allowed?('SELECT * FROM users')).to be false
      end
    end

    context 'when ActiveRecord is not available' do
      before do
        hide_const('ActiveRecord::Base') if defined?(ActiveRecord::Base)
      end

      it 'returns true (allows query)' do
        expect(gate.allowed?('SELECT * FROM users')).to be true
      end
    end

    context 'when explain plan is empty' do
      before do
        ar_base = Class.new do
          def self.connected?
            true
          end
        end
        stub_const('ActiveRecord::Base', ar_base)
        allow(gate).to receive(:get_explain_plan).and_return([])
      end

      it 'returns true' do
        expect(gate.allowed?('SELECT * FROM users')).to be true
      end
    end

    context 'when explain plan is nil' do
      before do
        ar_base = Class.new do
          def self.connected?
            true
          end
        end
        stub_const('ActiveRecord::Base', ar_base)
        allow(gate).to receive(:get_explain_plan).and_return(nil)
      end

      it 'returns true' do
        expect(gate.allowed?('SELECT * FROM users')).to be true
      end
    end

    context 'when an error occurs and explain_fail_open is true' do
      before do
        ar_base = Class.new do
          def self.connected?
            true
          end
        end
        stub_const('ActiveRecord::Base', ar_base)
        allow(gate).to receive(:get_explain_plan).and_raise(StandardError, 'connection error')
      end

      it 'returns true (fail-open)' do
        expect(gate.allowed?('SELECT * FROM users')).to be true
      end
    end

    context 'when an error occurs and explain_fail_open is false' do
      before do
        config.explain_fail_open = false
        ar_base = Class.new do
          def self.connected?
            true
          end
        end
        stub_const('ActiveRecord::Base', ar_base)
        allow(gate).to receive(:get_explain_plan).and_raise(StandardError, 'connection error')
      end

      it 'returns false (fail-closed)' do
        expect(gate.allowed?('SELECT * FROM users')).to be false
      end
    end
  end

  describe '#build_explain_query' do
    context 'with PostgreSQL adapter' do
      before { config.adapter = :postgres }

      it 'builds EXPLAIN with JSON format' do
        sql = gate.send(:build_explain_query, 'SELECT * FROM users')
        expect(sql).to include('EXPLAIN (ANALYZE false, BUFFERS false, VERBOSE false, FORMAT JSON)')
        expect(sql).to include('SELECT * FROM users')
      end
    end

    context 'with MySQL adapter' do
      before { config.adapter = :mysql }

      it 'builds EXPLAIN with JSON format' do
        sql = gate.send(:build_explain_query, 'SELECT * FROM users')
        expect(sql).to eq('EXPLAIN FORMAT=JSON SELECT * FROM users')
      end
    end

    context 'with SQLite adapter' do
      before { config.adapter = :sqlite }

      it 'builds EXPLAIN QUERY PLAN' do
        sql = gate.send(:build_explain_query, 'SELECT * FROM users')
        expect(sql).to eq('EXPLAIN QUERY PLAN SELECT * FROM users')
      end
    end

    context 'with unknown adapter' do
      before { config.adapter = :unknown }

      it 'builds simple EXPLAIN' do
        sql = gate.send(:build_explain_query, 'SELECT * FROM users')
        expect(sql).to eq('EXPLAIN SELECT * FROM users')
      end
    end
  end

  describe '#normalize_explain_result' do
    it 'handles array of hashes' do
      result = [{ 'Plan' => 'something' }]
      normalized = gate.send(:normalize_explain_result, result)
      expect(normalized).to eq([{ 'Plan' => 'something' }])
    end

    it 'handles array of arrays' do
      result = [['plan text']]
      normalized = gate.send(:normalize_explain_result, result)
      expect(normalized).to eq(['plan text'])
    end

    it 'handles non-array result' do
      result = 'plan text'
      normalized = gate.send(:normalize_explain_result, result)
      expect(normalized).to eq(['plan text'])
    end
  end

  describe '#check_node_safety' do
    context 'when cost exceeds limit' do
      let(:node) do
        {
          'Node Type' => 'Seq Scan',
          'Total Cost' => 15_000,
          'Plan Rows' => 100
        }
      end

      it 'returns false' do
        expect(gate.send(:check_node_safety, node)).to be false
      end
    end

    context 'when rows exceed limit' do
      let(:node) do
        {
          'Node Type' => 'Seq Scan',
          'Total Cost' => 100,
          'Plan Rows' => 200_000
        }
      end

      it 'returns false' do
        expect(gate.send(:check_node_safety, node)).to be false
      end
    end

    context 'with Seq Scan on large table' do
      let(:node) do
        {
          'Node Type' => 'Seq Scan',
          'Total Cost' => 100,
          'Plan Rows' => 5000
        }
      end

      it 'returns false when seq scans are not allowed' do
        expect(gate.send(:check_node_safety, node)).to be false
      end

      it 'returns true when seq scans are allowed' do
        config.allow_seq_scans = true
        expect(gate.send(:check_node_safety, node)).to be true
      end
    end

    context 'with Seq Scan on small table' do
      let(:node) do
        {
          'Node Type' => 'Seq Scan',
          'Total Cost' => 10,
          'Plan Rows' => 500
        }
      end

      it 'returns true even when seq scans are not allowed' do
        expect(gate.send(:check_node_safety, node)).to be true
      end
    end

    context 'with expensive Nested Loop' do
      let(:node) do
        {
          'Node Type' => 'Nested Loop',
          'Total Cost' => 500,
          'Plan Rows' => 50_000
        }
      end

      it 'returns false' do
        expect(gate.send(:check_node_safety, node)).to be false
      end
    end

    context 'with cheap Nested Loop' do
      let(:node) do
        {
          'Node Type' => 'Nested Loop',
          'Total Cost' => 100,
          'Plan Rows' => 1000
        }
      end

      it 'returns true' do
        expect(gate.send(:check_node_safety, node)).to be true
      end
    end

    context 'with child nodes' do
      let(:parent_node) do
        {
          'Node Type' => 'Hash Join',
          'Total Cost' => 100,
          'Plan Rows' => 100,
          'Plans' => [
            {
              'Node Type' => 'Seq Scan',
              'Total Cost' => 50,
              'Plan Rows' => 5000
            }
          ]
        }
      end

      it 'recursively checks child nodes' do
        expect(gate.send(:check_node_safety, parent_node)).to be false
      end
    end

    context 'with safe child nodes' do
      let(:parent_node) do
        {
          'Node Type' => 'Hash Join',
          'Total Cost' => 100,
          'Plan Rows' => 100,
          'Plans' => [
            {
              'Node Type' => 'Index Scan',
              'Total Cost' => 10,
              'Plan Rows' => 100
            }
          ]
        }
      end

      it 'returns true' do
        expect(gate.send(:check_node_safety, parent_node)).to be true
      end
    end

    context 'with nil node' do
      it 'returns true' do
        expect(gate.send(:check_node_safety, nil)).to be true
      end
    end

    context 'with non-hash node' do
      it 'returns true' do
        expect(gate.send(:check_node_safety, 'string')).to be true
      end
    end
  end

  describe '#analyze_postgres_json_plan' do
    context 'with valid JSON plan' do
      let(:plan) do
        [{
          'QUERY PLAN' => [{
            'Plan' => {
              'Node Type' => 'Index Scan',
              'Total Cost' => 100,
              'Plan Rows' => 50
            }
          }]
        }]
      end

      it 'returns true for safe plan' do
        expect(gate.send(:analyze_postgres_json_plan, plan.first['QUERY PLAN'])).to be true
      end
    end

    context 'with expensive plan' do
      let(:plan) do
        [{
          'Plan' => {
            'Node Type' => 'Seq Scan',
            'Total Cost' => 50_000,
            'Plan Rows' => 1_000_000
          }
        }]
      end

      it 'returns false' do
        expect(gate.send(:analyze_postgres_json_plan, plan)).to be false
      end
    end

    context 'with invalid plan structure' do
      it 'returns true for non-array' do
        expect(gate.send(:analyze_postgres_json_plan, 'invalid')).to be true
      end

      it 'returns true for array without hash' do
        expect(gate.send(:analyze_postgres_json_plan, ['string'])).to be true
      end

      it 'returns true for hash without Plan key' do
        expect(gate.send(:analyze_postgres_json_plan, [{ 'Other' => 'data' }])).to be true
      end
    end
  end

  describe '#analyze_postgres_text_plan' do
    context 'with seq scan' do
      let(:plan) { ['Seq Scan on users (cost=0.00..1000.00)'] }

      it 'returns false when seq scans not allowed' do
        expect(gate.send(:analyze_postgres_text_plan, plan)).to be false
      end

      it 'returns true when seq scans allowed' do
        config.allow_seq_scans = true
        expect(gate.send(:analyze_postgres_text_plan, plan)).to be true
      end
    end

    context 'with expensive sort' do
      let(:plan) { ['Sort (cost=10000.00..10500.00)'] }

      it 'returns false' do
        expect(gate.send(:analyze_postgres_text_plan, plan)).to be false
      end
    end

    context 'with expensive hash join' do
      let(:plan) { ['Hash Join (cost=5000.00..15000.00)'] }

      it 'returns false' do
        expect(gate.send(:analyze_postgres_text_plan, plan)).to be false
      end
    end

    context 'with safe plan' do
      let(:plan) { ['Index Scan using users_pkey on users (cost=0.00..8.27)'] }

      it 'returns true' do
        expect(gate.send(:analyze_postgres_text_plan, plan)).to be true
      end
    end
  end

  describe '#analyze_mysql_plan' do
    context 'with full table scan' do
      let(:plan) { ['full table scan on users'] }

      it 'returns false' do
        expect(gate.send(:analyze_mysql_plan, plan)).to be false
      end
    end

    context 'with filesort' do
      let(:plan) { ['Using filesort'] }

      it 'returns false' do
        expect(gate.send(:analyze_mysql_plan, plan)).to be false
      end
    end

    context 'with safe plan' do
      let(:plan) { ['Using index'] }

      it 'returns true' do
        expect(gate.send(:analyze_mysql_plan, plan)).to be true
      end
    end
  end

  describe '#analyze_sqlite_plan' do
    context 'with table scan' do
      let(:plan) { ['SCAN TABLE users'] }

      it 'returns false' do
        expect(gate.send(:analyze_sqlite_plan, plan)).to be false
      end
    end

    context 'with index scan' do
      let(:plan) { ['SEARCH TABLE users USING INDEX users_email_idx'] }

      it 'returns true' do
        expect(gate.send(:analyze_sqlite_plan, plan)).to be true
      end
    end
  end

  describe '#analyze_generic_plan' do
    context 'with full scan pattern' do
      let(:plan) { ['FULL TABLE SCAN'] }

      it 'returns false' do
        expect(gate.send(:analyze_generic_plan, plan)).to be false
      end
    end

    context 'with seq scan pattern' do
      let(:plan) { ['SEQ SCAN on table'] }

      it 'returns false' do
        expect(gate.send(:analyze_generic_plan, plan)).to be false
      end
    end

    context 'with high cost pattern' do
      let(:plan) { ['cost=50000'] }

      it 'returns false' do
        expect(gate.send(:analyze_generic_plan, plan)).to be false
      end
    end

    context 'with safe plan' do
      let(:plan) { ['INDEX SCAN using primary key'] }

      it 'returns true' do
        expect(gate.send(:analyze_generic_plan, plan)).to be true
      end
    end
  end

  describe 'threshold configuration' do
    it 'uses default max cost when not configured' do
      config.max_query_cost = nil
      node = { 'Node Type' => 'Seq Scan', 'Total Cost' => 15_000, 'Plan Rows' => 100 }

      expect(gate.send(:check_node_safety, node)).to be false
    end

    it 'uses default max rows when not configured' do
      config.max_query_rows = nil
      node = { 'Node Type' => 'Seq Scan', 'Total Cost' => 100, 'Plan Rows' => 150_000 }

      expect(gate.send(:check_node_safety, node)).to be false
    end

    it 'respects custom max cost' do
      config.max_query_cost = 50_000
      node = { 'Node Type' => 'Hash Join', 'Total Cost' => 30_000, 'Plan Rows' => 100 }

      expect(gate.send(:check_node_safety, node)).to be true
    end

    it 'respects custom max rows' do
      config.max_query_rows = 500_000
      node = { 'Node Type' => 'Hash Join', 'Total Cost' => 100, 'Plan Rows' => 300_000 }

      expect(gate.send(:check_node_safety, node)).to be true
    end
  end
end
