# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery::Compiler do
  let(:config) { stub_config(adapter: :postgres, default_limit: 100) }
  let(:compiler) { described_class.new(config) }

  describe '#compile' do
    context 'with basic SELECT query' do
      let(:intent) do
        {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['*'],
          'filters' => [],
          'order' => [],
          'limit' => 50,
          'params' => {}
        }
      end

      it 'generates basic SQL with LIMIT' do
        result = compiler.compile(intent)

        expect(result[:sql]).to include('SELECT * FROM "users"')
        expect(result[:sql]).to include('LIMIT 50')
        expect(result[:params]).to eq({})
        expect(result[:bind_spec]).to eq([])
      end
    end

    context 'with inequality filters' do
      let(:intent) do
        {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['*'],
          'filters' => [
            { 'column' => 'id', 'op' => '!=', 'param' => 'uid' },
            { 'column' => 'age', 'op' => '<>', 'param' => 'age' }
          ],
          'limit' => 100,
          'params' => { 'uid' => 1, 'age' => 30 }
        }
      end

      it 'generates WHERE with != and <> operators' do
        result = compiler.compile(intent)

        expect(result[:sql]).to include('"id" != $1')
        expect(result[:sql]).to include('"age" <> $2')
        expect(result[:bind_spec]).to include(hash_including(key: 'uid', column: 'id'))
        expect(result[:bind_spec]).to include(hash_including(key: 'age', column: 'age'))
      end
    end

    context 'with NOT EXISTS anti-join filter' do
      let(:intent) do
        {
          'type' => 'select',
          'table' => 'questions',
          'columns' => ['*'],
          'filters' => [
            {
              'column' => 'id',
              'op' => 'not_exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => [
                { 'column' => 'student_id', 'op' => '=', 'param' => 'student_id' }
              ]
            }
          ],
          'limit' => 100,
          'params' => { 'student_id' => 17_963 }
        }
      end

      it 'generates a correlated NOT EXISTS subquery' do
        result = compiler.compile(intent)

        expect(result[:sql]).to include('NOT EXISTS (SELECT 1 FROM "answers"')
        expect(result[:sql]).to include('"answers"."question_id" = "questions"."id"')
        expect(result[:sql]).to include('"answers"."student_id" = $1')
        expect(result[:bind_spec]).to include(hash_including(key: 'student_id', column: 'student_id'))
      end
    end

    context 'with WHERE filters' do
      let(:intent) do
        {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['*'],
          'filters' => [
            {
              'column' => 'active',
              'op' => '=',
              'param' => 'active_status'
            }
          ],
          'limit' => 100,
          'params' => { 'active_status' => true }
        }
      end

      it 'generates parameterized WHERE clause' do
        result = compiler.compile(intent)

        expect(result[:sql]).to include('WHERE "active" = $1')
        expect(result[:bind_spec]).to include(
          hash_including(key: 'active_status', column: 'active')
        )
      end
    end

    context 'with BETWEEN filters' do
      let(:intent) do
        {
          'type' => 'select',
          'table' => 'orders',
          'columns' => ['*'],
          'filters' => [
            {
              'column' => 'created_at',
              'op' => 'between',
              'param_start' => 'start_date',
              'param_end' => 'end_date'
            }
          ],
          'limit' => 100,
          'params' => {
            'start_date' => '2023-01-01',
            'end_date' => '2023-12-31'
          }
        }
      end

      it 'generates BETWEEN clause with two parameters' do
        result = compiler.compile(intent)

        expect(result[:sql]).to include('WHERE "created_at" BETWEEN $1 AND $2')
        expect(result[:bind_spec]).to include(
          hash_including(key: 'start_date', column: 'created_at'),
          hash_including(key: 'end_date', column: 'created_at')
        )
      end
    end

    context 'with ORDER BY clause' do
      let(:intent) do
        {
          'type' => 'select',
          'table' => 'users',
          'columns' => ['*'],
          'order' => [
            { 'column' => 'created_at', 'dir' => 'desc' },
            { 'column' => 'email', 'dir' => 'asc' }
          ],
          'limit' => 100
        }
      end

      it 'generates ORDER BY clause' do
        result = compiler.compile(intent)

        expect(result[:sql]).to include('ORDER BY "created_at" DESC, "email" ASC')
      end
    end

    context 'with different database adapters' do
      it 'uses correct placeholder syntax for PostgreSQL' do
        config.adapter = :postgres
        result = compiler.compile({
                                    'table' => 'users',
                                    'filters' => [{ 'column' => 'id', 'op' => '=', 'param' => 'user_id' }],
                                    'limit' => 100
                                  })

        expect(result[:sql]).to include('$1')
      end

      it 'uses correct placeholder syntax for MySQL' do
        config.adapter = :mysql
        result = compiler.compile({
                                    'table' => 'users',
                                    'filters' => [{ 'column' => 'id', 'op' => '=', 'param' => 'user_id' }],
                                    'limit' => 100
                                  })

        expect(result[:sql]).to include('?')
      end
    end

    context 'with column selection' do
      let(:intent) do
        {
          'table' => 'users',
          'columns' => %w[id email created_at],
          'limit' => 100
        }
      end

      it 'selects specific columns' do
        result = compiler.compile(intent)

        expect(result[:sql]).to include('SELECT "id", "email", "created_at"')
      end
    end

    context 'with GROUP BY clause' do
      let(:intent) do
        {
          'table' => 'orders',
          'columns' => ['user_id', 'COUNT(*)'],
          'group_by' => ['user_id'],
          'limit' => 100
        }
      end

      it 'generates GROUP BY clause' do
        result = compiler.compile(intent)

        expect(result[:sql]).to include('GROUP BY "user_id"')
        expect(result[:sql]).to include('COUNT(*)')
      end
    end

    context 'with GROUP BY on multiple columns' do
      let(:intent) do
        {
          'table' => 'orders',
          'columns' => ['user_id', 'status', 'COUNT(*)'],
          'group_by' => %w[user_id status],
          'limit' => 100
        }
      end

      it 'generates GROUP BY with multiple columns' do
        result = compiler.compile(intent)

        expect(result[:sql]).to include('GROUP BY "user_id", "status"')
      end
    end

    context 'with HAVING clause' do
      let(:intent) do
        {
          'table' => 'orders',
          'columns' => ['user_id', 'COUNT(*)'],
          'group_by' => ['user_id'],
          'having' => [
            { 'function' => 'count', 'column' => nil, 'op' => '>', 'param' => 'min_orders' }
          ],
          'limit' => 100,
          'params' => { 'min_orders' => 5 }
        }
      end

      it 'generates HAVING clause with COUNT' do
        result = compiler.compile(intent)

        expect(result[:sql]).to include('GROUP BY "user_id"')
        expect(result[:sql]).to include('HAVING COUNT(*) > $1')
        expect(result[:bind_spec]).to include(hash_including(key: 'min_orders'))
      end
    end

    context 'with HAVING clause using SUM' do
      let(:intent) do
        {
          'table' => 'orders',
          'columns' => ['user_id', 'SUM(amount)'],
          'group_by' => ['user_id'],
          'having' => [
            { 'function' => 'sum', 'column' => 'amount', 'op' => '>=', 'param' => 'min_amount' }
          ],
          'limit' => 100,
          'params' => { 'min_amount' => 1000 }
        }
      end

      it 'generates HAVING clause with SUM' do
        result = compiler.compile(intent)

        expect(result[:sql]).to include('HAVING SUM("amount") >= $1')
        expect(result[:bind_spec]).to include(hash_including(key: 'min_amount'))
      end
    end

    context 'with IN clause' do
      let(:intent) do
        {
          'table' => 'users',
          'columns' => ['*'],
          'filters' => [
            { 'column' => 'status', 'op' => 'in', 'param' => 'statuses' }
          ],
          'limit' => 100,
          'params' => { 'statuses' => %w[active pending] }
        }
      end

      it 'generates IN clause' do
        result = compiler.compile(intent)

        expect(result[:sql]).to include('"status" IN ($1)')
        expect(result[:bind_spec]).to include(hash_including(key: 'statuses', cast: :array))
      end
    end

    context 'with empty array in IN clause' do
      let(:intent) do
        {
          'table' => 'users',
          'columns' => ['*'],
          'filters' => [
            { 'column' => 'status', 'op' => 'in', 'param' => 'statuses' }
          ],
          'limit' => 100,
          'params' => { 'statuses' => [] }
        }
      end

      it 'raises an error for empty array' do
        expect { compiler.compile(intent) }.to raise_error(
          ArgumentError, /IN clause requires non-empty array/
        )
      end
    end

    context 'with LIKE filter' do
      let(:intent) do
        {
          'table' => 'users',
          'columns' => ['*'],
          'filters' => [
            { 'column' => 'email', 'op' => 'like', 'param' => 'email_pattern' }
          ],
          'limit' => 100,
          'params' => { 'email_pattern' => '%@example.com' }
        }
      end

      it 'generates LIKE clause' do
        result = compiler.compile(intent)

        expect(result[:sql]).to include('"email" LIKE $1')
      end
    end

    context 'with ILIKE filter (PostgreSQL)' do
      let(:intent) do
        {
          'table' => 'users',
          'columns' => ['*'],
          'filters' => [
            { 'column' => 'name', 'op' => 'ilike', 'param' => 'name_pattern' }
          ],
          'limit' => 100,
          'params' => { 'name_pattern' => '%john%' }
        }
      end

      it 'generates ILIKE clause' do
        result = compiler.compile(intent)

        expect(result[:sql]).to include('"name" ILIKE $1')
      end
    end

    context 'with aggregate functions' do
      let(:intent) do
        {
          'table' => 'orders',
          'columns' => ['SUM(amount)'],
          'limit' => 100
        }
      end

      it 'generates SUM aggregate' do
        result = compiler.compile(intent)

        expect(result[:sql]).to include('SUM("amount") as sum')
      end
    end

    context 'with AVG aggregate' do
      let(:intent) do
        {
          'table' => 'orders',
          'columns' => ['AVG(amount)'],
          'limit' => 100
        }
      end

      it 'generates AVG aggregate' do
        result = compiler.compile(intent)

        expect(result[:sql]).to include('AVG("amount") as avg')
      end
    end

    context 'with multiple filters combined' do
      let(:intent) do
        {
          'table' => 'users',
          'columns' => ['*'],
          'filters' => [
            { 'column' => 'active', 'op' => '=', 'param' => 'is_active' },
            { 'column' => 'age', 'op' => '>=', 'param' => 'min_age' },
            { 'column' => 'role', 'op' => 'in', 'param' => 'roles' }
          ],
          'limit' => 100,
          'params' => {
            'is_active' => true,
            'min_age' => 18,
            'roles' => %w[admin moderator]
          }
        }
      end

      it 'generates WHERE with AND logic' do
        result = compiler.compile(intent)

        expect(result[:sql]).to include('"active" = $1')
        expect(result[:sql]).to include('"age" >= $2')
        expect(result[:sql]).to include('"role" IN ($3)')
        expect(result[:sql]).to include(' AND ')
      end
    end

    context 'with GROUP BY, HAVING, and ORDER BY combined' do
      let(:intent) do
        {
          'table' => 'orders',
          'columns' => ['user_id', 'COUNT(*)'],
          'group_by' => ['user_id'],
          'having' => [
            { 'function' => 'count', 'column' => nil, 'op' => '>', 'param' => 'min_orders' }
          ],
          'order' => [{ 'column' => 'user_id', 'dir' => 'asc' }],
          'limit' => 100,
          'params' => { 'min_orders' => 10 }
        }
      end

      it 'generates correct clause order' do
        result = compiler.compile(intent)
        sql = result[:sql]

        group_pos = sql.index('GROUP BY')
        having_pos = sql.index('HAVING')
        order_pos = sql.index('ORDER BY')
        limit_pos = sql.index('LIMIT')

        expect(group_pos).to be < having_pos
        expect(having_pos).to be < order_pos
        expect(order_pos).to be < limit_pos
      end
    end
  end
end
