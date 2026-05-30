# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery::Validator do
  let(:config) { stub_config(adapter: :postgres, default_limit: 100) }
  let(:validator) { described_class.new }

  describe '#validate' do
    context 'with valid basic intent' do
      let(:intent) do
        {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['*']
        }
      end

      it 'returns validated intent with default limit' do
        result = validator.validate(intent)

        expect(result[:type]).to eq('select')
        expect(result[:table]).to eq('users')
        expect(result[:columns]).to eq(['*'])
        expect(result[:limit]).to eq(CodeToQuery.config.default_limit)
      end
    end

    context 'with missing required fields' do
      it 'raises ArgumentError when type is missing' do
        intent = { 'table' => 'users', 'columns' => ['*'] }

        expect { validator.validate(intent) }.to raise_error(ArgumentError, /type/)
      end

      it 'raises ArgumentError when table is missing' do
        intent = { 'type' => 'select', 'columns' => ['*'] }

        expect { validator.validate(intent) }.to raise_error(ArgumentError, /table/)
      end

      it 'raises ArgumentError when columns is missing' do
        intent = { 'type' => 'select', 'table' => 'users' }

        expect { validator.validate(intent) }.to raise_error(ArgumentError, /columns/)
      end
    end

    context 'with filters' do
      it 'validates basic equality filter' do
        intent = {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['*'],
          'filters' => [
            { 'column' => 'id', 'op' => '=', 'param' => 'user_id' }
          ]
        }

        result = validator.validate(intent)
        expect(result[:filters].first[:op]).to eq('=')
      end

      it 'validates between filter with param_start and param_end' do
        intent = {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['*'],
          'filters' => [
            { 'column' => 'created_at', 'op' => 'between', 'param_start' => 'start', 'param_end' => 'end' }
          ]
        }

        result = validator.validate(intent)
        expect(result[:filters].first[:op]).to eq('between')
      end
    end

    context 'with exists filter' do
      it 'validates exists filter with related_table and fk_column' do
        intent = {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['*'],
          'filters' => [
            {
              'op' => 'exists',
              'related_table' => 'orders',
              'fk_column' => 'user_id',
              'related_filters' => [
                { 'column' => 'status', 'op' => '=', 'param' => 'order_status' }
              ]
            }
          ]
        }

        result = validator.validate(intent)
        expect(result[:filters].first[:op]).to eq('exists')
        expect(result[:filters].first[:related_table]).to eq('orders')
      end
    end

    context 'with not_exists filter' do
      it 'validates not_exists filter' do
        intent = {
          'type' => 'select',
          'table' => 'questions',
          'columns' => ['*'],
          'filters' => [
            {
              'op' => 'not_exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => [
                { 'column' => 'student_id', 'op' => '=', 'param' => 'student' }
              ]
            }
          ]
        }

        result = validator.validate(intent)
        expect(result[:filters].first[:op]).to eq('not_exists')
      end
    end

    context 'with order clause' do
      it 'validates order clause' do
        intent = {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['*'],
          'order' => [
            { 'column' => 'created_at', 'dir' => 'desc' }
          ]
        }

        result = validator.validate(intent)
        expect(result[:order].first[:column]).to eq('created_at')
        expect(result[:order].first[:dir]).to eq('desc')
      end

      it 'validates multiple order columns' do
        intent = {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['*'],
          'order' => [
            { 'column' => 'name', 'dir' => 'asc' },
            { 'column' => 'created_at', 'dir' => 'desc' }
          ]
        }

        result = validator.validate(intent)
        expect(result[:order].length).to eq(2)
      end
    end

    context 'with distinct' do
      it 'validates distinct flag' do
        intent = {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['email'],
          'distinct' => true
        }

        result = validator.validate(intent)
        expect(result[:distinct]).to be true
      end

      it 'validates distinct_on array' do
        intent = {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['*'],
          'distinct' => true,
          'distinct_on' => ['user_id']
        }

        result = validator.validate(intent)
        expect(result[:distinct_on]).to eq(['user_id'])
      end
    end

    context 'with aggregations' do
      it 'validates aggregation with type and column' do
        intent = {
          'type' => 'select',
          'table' => 'orders',
          'columns' => ['*'],
          'aggregations' => [
            { 'type' => 'sum', 'column' => 'amount' }
          ]
        }

        result = validator.validate(intent)
        expect(result[:aggregations].first[:type]).to eq('sum')
      end

      it 'validates count aggregation without column' do
        intent = {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['*'],
          'aggregations' => [
            { 'type' => 'count' }
          ]
        }

        result = validator.validate(intent)
        expect(result[:aggregations].first[:type]).to eq('count')
      end
    end

    context 'with group_by' do
      it 'validates group_by columns' do
        intent = {
          'type' => 'select',
          'table' => 'orders',
          'columns' => ['user_id'],
          'group_by' => ['user_id']
        }

        result = validator.validate(intent)
        expect(result[:group_by]).to eq(['user_id'])
      end
    end

    context 'with metrics' do
      it 'preserves _metrics from intent' do
        intent = {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['*'],
          '_metrics' => { 'prompt_tokens' => 100, 'elapsed_s' => 0.5 }
        }

        result = validator.validate(intent)
        expect(result['_metrics']).to eq({ 'prompt_tokens' => 100, 'elapsed_s' => 0.5 })
      end
    end

    context 'with allow_tables restriction' do
      it 'allows table when in allow_tables list' do
        intent = {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['*']
        }

        result = validator.validate(intent, allow_tables: %w[users orders])
        expect(result[:table]).to eq('users')
      end

      it 'performs case-insensitive table matching' do
        intent = {
          'type' => 'select',
          'table' => 'Users',
          'columns' => ['*']
        }

        result = validator.validate(intent, allow_tables: ['users'])
        expect(result[:table]).to eq('Users')
      end
    end

    context 'with policy adapter' do
      let(:policy_adapter) do
        lambda do |_user, **_kwargs|
          {
            allowed_tables: %w[users orders],
            allowed_columns: {
              'users' => %w[id email name],
              'orders' => %w[id user_id total]
            }
          }
        end
      end

      before do
        config.policy_adapter = policy_adapter
      end

      it 'allows table permitted by policy' do
        intent = {
          'type' => 'select',
          'table' => 'users',
          'columns' => %w[id email]
        }

        result = validator.validate(intent)
        expect(result[:table]).to eq('users')
      end

      it 'allows selecting wildcard column' do
        intent = {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['*']
        }

        result = validator.validate(intent)
        expect(result[:columns]).to eq(['*'])
      end
    end
  end

  describe '#preprocess_exists_filters' do
    it 'adds default column for exists filters without column' do
      intent = {
        'filters' => [
          { 'op' => 'exists', 'related_table' => 'orders', 'fk_column' => 'user_id' }
        ]
      }

      result = validator.send(:preprocess_exists_filters, intent)
      expect(result['filters'].first['column']).to eq('id')
    end

    it 'preserves existing column for exists filters' do
      intent = {
        'filters' => [
          { 'op' => 'exists', 'column' => 'custom_id', 'related_table' => 'orders', 'fk_column' => 'user_id' }
        ]
      }

      result = validator.send(:preprocess_exists_filters, intent)
      expect(result['filters'].first['column']).to eq('custom_id')
    end

    it 'handles non-array filters gracefully' do
      intent = { 'filters' => nil }

      result = validator.send(:preprocess_exists_filters, intent)
      expect(result['filters']).to be_nil
    end
  end

  describe '#safe_call_policy_adapter' do
    context 'when adapter accepts all arguments' do
      let(:adapter) do
        ->(user, table:, intent:) { { allowed_tables: ['users'] } } # rubocop:disable Lint/UnusedBlockArgument
      end

      it 'calls adapter with full arguments' do
        result = validator.send(:safe_call_policy_adapter, adapter, nil, table: 'users', intent: {})
        expect(result[:allowed_tables]).to eq(['users'])
      end
    end

    context 'when adapter only accepts user and table' do
      let(:adapter) do
        ->(user, table:) { { allowed_tables: ['orders'] } } # rubocop:disable Lint/UnusedBlockArgument
      end

      it 'falls back to simpler call signature' do
        result = validator.send(:safe_call_policy_adapter, adapter, nil, table: 'orders', intent: {})
        expect(result[:allowed_tables]).to eq(['orders'])
      end
    end

    context 'when adapter only accepts current user' do
      let(:adapter) do
        ->(user) { { allowed_tables: [user.fetch(:table)] } }
      end

      it 'falls back to current-user-only call signature under fail-closed default' do
        CodeToQuery.config.policy_adapter_fail_open = false

        result = validator.send(
          :safe_call_policy_adapter,
          adapter,
          { table: 'accounts' },
          table: 'orders',
          intent: {}
        )

        expect(result[:allowed_tables]).to eq(['accounts'])
      end
    end

    context 'when adapter raises error' do
      let(:adapter) do
        ->(_user, **_kwargs) { raise StandardError, 'adapter error' }
      end

      after do
        CodeToQuery.config.policy_adapter_fail_open = false
      end

      it 'fails closed by default' do
        expect do
          validator.send(:safe_call_policy_adapter, adapter, nil, table: 'users', intent: {})
        end.to raise_error(CodeToQuery::PolicyAdapterError, /Policy adapter failed: adapter error/)
      end

      it 'returns empty hash only in explicit availability mode' do
        CodeToQuery.config.policy_adapter_fail_open = true

        result = validator.send(:safe_call_policy_adapter, adapter, nil, table: 'users', intent: {})

        expect(result).to eq({})
      end
    end
  end
end
