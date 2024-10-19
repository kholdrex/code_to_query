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
  end
end
