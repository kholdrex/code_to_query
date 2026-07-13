# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery::Validator do
  let(:config) { stub_config(adapter: :postgres, default_limit: 100) }
  let(:validator) { described_class.new }

  after do
    CodeToQuery.config.policy_adapter = nil
    CodeToQuery.config.policy_adapter_fail_open = false
  end

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

    context 'with policy table restrictions' do
      let(:intent) do
        { 'type' => 'select', 'table' => 'users', 'columns' => ['*'],
          '__policy_allowed_tables' => ['users'] }
      end

      it 'strips caller-supplied policy allowlist metadata before consulting policy' do
        CodeToQuery.config.policy_adapter = ->(_user, **) { { allowed_tables: ['orders'] } }

        expect { validator.validate(intent) }.to raise_error(ArgumentError, /not permitted by policy/)
      end

      it 'treats an explicitly empty policy allowlist as deny all' do
        CodeToQuery.config.policy_adapter = ->(_user, **) { { allowed_tables: [] } }

        expect { validator.validate(intent) }.to raise_error(ArgumentError, /not permitted by policy/)
      end

      it 'leaves tables unrestricted when the policy table allowlist is absent' do
        CodeToQuery.config.policy_adapter = ->(_user, **) { { allowed_columns: {} } }

        result = validator.validate(intent)

        expect(result[:table]).to eq('users')
        expect(result).not_to have_key(:__policy_allowed_tables)
        expect(result).not_to have_key('__policy_allowed_tables')
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

      it 'adds default exists columns using the validated intent key style' do
        intent = {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['*'],
          'filters' => [
            {
              'op' => 'exists',
              'related_table' => 'orders',
              'fk_column' => 'user_id'
            }
          ]
        }

        result = validator.validate(intent)
        filter = result[:filters].first

        expect(filter[:column]).to eq('id')
        expect(filter[:base_column]).to eq('id')
        expect(filter).not_to include('column', 'base_column')
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

    context 'with policy column restrictions on related subqueries' do
      before do
        config.policy_adapter = lambda do |_user, **_kwargs|
          {
            allowed_tables: %w[users orders],
            allowed_columns: {
              'users' => %w[id email],
              'orders' => %w[user_id status]
            }
          }
        end
      end

      def related_subquery_intent(operator, fk_column: 'user_id', base_column: 'id')
        {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['email'],
          'filters' => [
            {
              'op' => operator,
              'related_table' => 'orders',
              'fk_column' => fk_column,
              'base_column' => base_column,
              'related_filters' => [
                { 'column' => 'status', 'op' => '=', 'param' => 'status' }
              ]
            }
          ]
        }
      end

      %w[exists not_exists].each do |op|
        it "allows #{op} when both correlated columns are policy-permitted" do
          result = validator.validate(related_subquery_intent(op))

          expect(result[:filters].first).to include(fk_column: 'user_id', base_column: 'id')
        end

        it "rejects #{op} when fk_column is not permitted on the related table" do
          expect do
            validator.validate(related_subquery_intent(op, fk_column: 'secret_user_id'))
          end.to raise_error(ArgumentError, /column 'secret_user_id' not permitted on 'orders'/)
        end

        it "rejects #{op} when base_column is not permitted on the main table" do
          expect do
            validator.validate(related_subquery_intent(op, base_column: 'secret_id'))
          end.to raise_error(ArgumentError, /column 'secret_id' not permitted on 'users'/)
        end

        it "uses and checks the default id base_column for #{op} when it is omitted" do
          subquery_intent = related_subquery_intent(op)
          subquery_intent['filters'].first.delete('base_column')

          result = validator.validate(subquery_intent)

          expect(result[:filters].first[:base_column]).to eq('id')
        end

        it "rejects #{op} when the omitted base_column defaults to a disallowed id" do
          config.policy_adapter = lambda do |_user, **_kwargs|
            {
              allowed_tables: %w[users orders],
              allowed_columns: {
                'users' => ['email'],
                'orders' => %w[user_id status]
              }
            }
          end
          subquery_intent = related_subquery_intent(op)
          subquery_intent['filters'].first.delete('base_column')

          expect do
            validator.validate(subquery_intent)
          end.to raise_error(ArgumentError, /column 'id' not permitted on 'users'/)
        end

        it "does not restrict #{op} correlation columns for nil or empty per-table lists" do
          config.policy_adapter = lambda do |_user, **_kwargs|
            {
              allowed_tables: %w[users orders],
              allowed_columns: { 'users' => nil, 'orders' => [] }
            }
          end

          result = validator.validate(
            related_subquery_intent(op, fk_column: 'legacy_user_key', base_column: 'legacy_id')
          )

          expect(result[:filters].first).to include(
            fk_column: 'legacy_user_key', base_column: 'legacy_id'
          )
        end
      end
    end

    context 'with adapter-specific policy column casing' do
      let(:column_policy) do
        {
          allowed_tables: %w[users orders],
          allowed_columns: {
            'users' => %w[id UserCode PublicTotal],
            'orders' => %w[user_id User_ID status StatusCode]
          }
        }
      end
      let(:related_intent) do
        {
          'type' => 'select', 'table' => 'users', 'columns' => ['UserCode'],
          'filters' => [{
            'op' => 'exists', 'related_table' => 'orders',
            'fk_column' => 'user_id', 'base_column' => 'id',
            'related_filters' => [{ 'column' => 'StatusCode', 'op' => '=', 'param' => 'status' }]
          }]
        }
      end

      before do
        config.policy_adapter = ->(_user, **_kwargs) { column_policy }
      end

      it 'allows distinct exact-case PostgreSQL columns' do
        expect { validator.validate(related_intent) }.not_to raise_error

        mixed_case_fk = related_intent.dup
        mixed_case_fk['filters'] = related_intent['filters'].map { |filter| filter.merge('fk_column' => 'User_ID') }
        expect { validator.validate(mixed_case_fk) }.not_to raise_error
      end

      {
        'selected column' => ->(intent) { intent['columns'] = ['usercode'] },
        'ORDER BY column' => lambda do |intent|
          intent['order'] = [{ 'column' => 'usercode', 'dir' => 'asc' }]
        end,
        'DISTINCT ON column' => ->(intent) { intent['distinct_on'] = ['usercode'] },
        'GROUP BY column' => ->(intent) { intent['group_by'] = ['usercode'] },
        'aggregation column' => lambda do |intent|
          intent['aggregations'] = [{ 'type' => 'sum', 'column' => 'publictotal' }]
        end,
        'main-table filter column' => lambda do |intent|
          intent['filters'] = [{ 'column' => 'usercode', 'op' => '=', 'param' => 'code' }]
        end,
        'related fk_column' => ->(intent) { intent['filters'].first['fk_column'] = 'USER_ID' },
        'main-table base_column' => ->(intent) { intent['filters'].first['base_column'] = 'ID' },
        'related filter column' => ->(intent) { intent['filters'].first['related_filters'].first['column'] = 'statuscode' }
      }.each do |path, change_case|
        it "rejects a case-only PostgreSQL mismatch in the #{path}" do
          intent = Marshal.load(Marshal.dump(related_intent))
          change_case.call(intent)

          expect { validator.validate(intent) }.to raise_error(ArgumentError, /not permitted/)
        end
      end

      %i[mysql sqlite].each do |adapter|
        it "retains #{adapter} case-insensitive column semantics" do
          config.adapter = adapter
          intent = Marshal.load(Marshal.dump(related_intent))
          intent['columns'] = ['usercode']
          intent['filters'].first['fk_column'] = 'USER_ID'
          intent['filters'].first['base_column'] = 'ID'
          intent['filters'].first['related_filters'].first['column'] = 'statuscode'

          expect { validator.validate(intent) }.not_to raise_error
        end
      end

      it 'uses case-insensitive MySQL policy table keys when enforcing columns' do
        config.adapter = :mysql
        config.policy_adapter = lambda do |_user, **_kwargs|
          { allowed_columns: { 'Users' => ['id'] } }
        end
        intent = { 'type' => 'select', 'table' => 'users', 'columns' => ['secret'] }

        expect { validator.validate(intent) }
          .to raise_error(ArgumentError, /selecting column 'secret' not permitted on 'users'/)
      end

      context 'with a case-only PostgreSQL policy table-key mismatch' do
        before do
          config.policy_adapter = lambda do |_user, **_kwargs|
            { allowed_columns: { 'Users' => ['id'] } }
          end
        end

        {
          'selected column' => ->(intent) { intent['columns'] = ['id'] },
          'filter column' => lambda do |intent|
            intent['filters'] = [{ 'column' => 'id', 'op' => '=', 'param' => 'id' }]
          end,
          'ORDER BY column' => lambda do |intent|
            intent['order'] = [{ 'column' => 'id', 'dir' => 'asc' }]
          end,
          'DISTINCT ON column' => ->(intent) { intent['distinct_on'] = ['id'] },
          'GROUP BY column' => ->(intent) { intent['group_by'] = ['id'] },
          'aggregation column' => lambda do |intent|
            intent['aggregations'] = [{ 'type' => 'sum', 'column' => 'id' }]
          end
        }.each do |path, add_column_reference|
          it "fails closed for the main-table #{path}" do
            intent = { 'type' => 'select', 'table' => 'users', 'columns' => ['*'] }
            add_column_reference.call(intent)

            expect { validator.validate(intent) }.to raise_error(ArgumentError, /not permitted on 'users'/)
          end
        end

        %w[fk_column related_filters].each do |path|
          it "fails closed for a related-table #{path} path" do
            config.policy_adapter = lambda do |_user, **_kwargs|
              { allowed_columns: { 'users' => ['id'], 'Orders' => %w[user_id status] } }
            end
            intent = {
              'type' => 'select', 'table' => 'users', 'columns' => ['*'],
              'filters' => [{
                'op' => 'exists', 'related_table' => 'orders',
                'fk_column' => 'user_id', 'base_column' => 'id',
                'related_filters' => [{ 'column' => 'status', 'op' => '=', 'param' => 'status' }]
              }]
            }

            expect { validator.validate(intent) }.to raise_error(ArgumentError, /not permitted on 'orders'/)
          end
        end

        it 'fails closed for a main-table base_column path' do
          config.policy_adapter = lambda do |_user, **_kwargs|
            { allowed_columns: { 'Users' => ['id'], 'orders' => ['user_id'] } }
          end
          intent = {
            'type' => 'select', 'table' => 'users', 'columns' => ['*'],
            'filters' => [{
              'op' => 'exists', 'related_table' => 'orders',
              'fk_column' => 'user_id', 'base_column' => 'id'
            }]
          }

          expect { validator.validate(intent) }.to raise_error(ArgumentError, /not permitted on 'users'/)
        end

        it 'preserves partial policies for truly unrelated absent PostgreSQL table keys' do
          config.policy_adapter = lambda do |_user, **_kwargs|
            { allowed_columns: { 'accounts' => ['id'] } }
          end
          intent = { 'type' => 'select', 'table' => 'users', 'columns' => ['secret'] }

          expect { validator.validate(intent) }.not_to raise_error
        end

        %i[mysql sqlite].each do |adapter|
          it "retains #{adapter} case-insensitive table-key behavior" do
            config.adapter = adapter
            intent = { 'type' => 'select', 'table' => 'users', 'columns' => ['id'] }

            expect { validator.validate(intent) }.not_to raise_error
          end
        end
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

      it 'records policy-allowed tables for downstream SQL linting' do
        intent = {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['*']
        }

        result = validator.validate(intent)
        expect(result[:__policy_allowed_tables]).to eq(%w[users orders])
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
        allow(adapter).to receive(:call).and_call_original

        result = validator.send(:safe_call_policy_adapter, adapter, nil, table: 'orders', intent: {})
        expect(result[:allowed_tables]).to eq(['orders'])
        expect(adapter).to have_received(:call).once
      end
    end

    context 'when adapter only accepts current user' do
      let(:adapter) do
        ->(user) { { allowed_tables: [user.fetch(:table)] } }
      end

      it 'falls back to current-user-only call signature under fail-closed default' do
        CodeToQuery.config.policy_adapter_fail_open = false
        allow(adapter).to receive(:call).and_call_original

        result = validator.send(
          :safe_call_policy_adapter,
          adapter,
          { table: 'accounts' },
          table: 'orders',
          intent: {}
        )

        expect(result[:allowed_tables]).to eq(['accounts'])
        expect(adapter).to have_received(:call).once
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

    context 'when a compatible adapter raises a signature-like ArgumentError internally' do
      ['wrong number of arguments', 'unknown keyword: :intent'].each do |message|
        it "invokes the adapter once and fails closed for #{message.inspect}" do
          calls = 0
          adapter = lambda do |_user, table:, intent:|
            calls += 1
            raise ArgumentError, message if table == 'users' && intent
          end

          expect do
            validator.send(:safe_call_policy_adapter, adapter, nil, table: 'users', intent: {})
          end.to raise_error(CodeToQuery::PolicyAdapterError, /Policy adapter failed: #{Regexp.escape(message)}/)
          expect(calls).to eq(1)
        end
      end
    end
  end
end
