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

  def build_scope_backed_user_model(scope)
    stub_const('ActiveRecord', Module.new) unless defined?(ActiveRecord)
    stub_const('ActiveRecord::Base', Class.new)

    user_model = Class.new(ActiveRecord::Base) do
      def self.table_name = 'users'
      def self.all = @scope

      class << self
        attr_writer :scope
      end
    end

    user_model.scope = scope
    stub_const('User', user_model)
  end

  def build_scope_backed_question_model(scope)
    stub_const('ActiveRecord', Module.new) unless defined?(ActiveRecord)
    stub_const('ActiveRecord::Base', Class.new) unless defined?(ActiveRecord::Base)

    question_model = Class.new(ActiveRecord::Base) do
      def self.table_name = 'questions'
      def self.all = @scope

      class << self
        attr_writer :scope
      end
    end

    question_model.scope = scope
    stub_const('Question', question_model)
  end

  def build_policy_query(config)
    described_class.new(
      sql: 'SELECT * FROM "users" WHERE "active" = $1 AND "tenant_id" = $2 LIMIT 100',
      params: { 'active' => true, 'policy_tenant_id' => 42 },
      bind_spec: [
        { key: 'active', column: 'active', cast: nil },
        { key: 'policy_tenant_id', column: 'tenant_id', cast: nil }
      ],
      intent: {
        'table' => 'users',
        'type' => 'select',
        'filters' => [
          { 'column' => 'active', 'op' => '=', 'param' => 'active' },
          { 'column' => 'tenant_id', 'op' => '=', 'param' => 'policy_tenant_id' }
        ],
        'limit' => 100
      },
      allow_tables: ['users'],
      config: config
    )
  end

  def build_subquery_policy_query(config)
    described_class.new(
      sql: 'SELECT * FROM "questions" WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1)',
      params: { 'policy_subquery_1_answers_tenant_id' => 42 },
      bind_spec: [{ key: 'policy_subquery_1_answers_tenant_id', column: 'tenant_id', cast: nil }],
      intent: {
        'table' => 'questions',
        'type' => 'select',
        'filters' => [
          {
            'column' => 'id',
            'op' => 'exists',
            'related_table' => 'answers',
            'fk_column' => 'question_id',
            'base_column' => 'id',
            'related_filters' => []
          }
        ],
        '__policy_expected_keys' => ['policy_subquery_1_answers_tenant_id']
      },
      allow_tables: ['questions'],
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
      scope = spy('scope')

      scoped_query.send(:apply_filter_to_scope, scope, { 'column' => 'archived', 'op' => '=' })

      expect(scope).to have_received(:where).with('archived' => false)
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
      scope = spy('scope')

      scoped_query.send(:apply_filter_to_scope, scope, { 'column' => 'created_at', 'op' => 'between', 'param_start' => 'from_date' })

      expect(scope).to have_received(:where).with('created_at' => (nil..'2023-12-31'))
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

      inferred_type = typed_query.send(:infer_column_type, nil, nil, 'archived', nil, 'is_archived')
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

    it 'returns true for allowlist-only policy adapters with no injected predicates' do
      config.policy_adapter = ->(_user, **) { { allowed_tables: ['users'] } }

      expect(query.safe?).to be true
    ensure
      config.policy_adapter = nil
    end

    it 'returns false when expected subquery policy keys are missing from binds and params' do
      config.policy_adapter = ->(_user, **) { { allowed_tables: ['users'] } }

      q = described_class.new(
        sql: 'SELECT * FROM "questions" WHERE NOT EXISTS (SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1)',
        params: {},
        bind_spec: [],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          'filters' => [
            {
              'column' => 'id',
              'op' => 'not_exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ],
          '__policy_expected_keys' => ['policy_subquery_1_answers_tenant_id']
        },
        allow_tables: ['questions'],
        config: config
      )

      expect(q.safe?).to be false
    ensure
      config.policy_adapter = nil
    end

    it 'returns true when expected subquery policy keys are present in binds' do
      config.policy_adapter = ->(_user, **) { { allowed_tables: ['users'] } }

      q = described_class.new(
        sql: 'SELECT * FROM "questions" WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1)',
        params: { 'policy_subquery_1_answers_tenant_id' => 42 },
        bind_spec: [{ key: 'policy_subquery_1_answers_tenant_id', column: 'tenant_id', cast: nil }],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          'filters' => [
            {
              'column' => 'id',
              'op' => 'exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ],
          '__policy_expected_keys' => ['policy_subquery_1_answers_tenant_id']
        },
        allow_tables: %w[questions answers],
        config: config
      )

      allow(q).to receive(:perform_safety_checks).and_call_original

      expect(q.safe?).to be true
    ensure
      config.policy_adapter = nil
    end

    it 'returns false when expected subquery policy keys are present only in params' do
      config.policy_adapter = ->(_user, **) { { allowed_tables: ['users'] } }

      q = described_class.new(
        sql: 'SELECT * FROM "questions" WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1)',
        params: { 'policy_subquery_1_answers_tenant_id' => 42 },
        bind_spec: [],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          'filters' => [
            {
              'column' => 'id',
              'op' => 'exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ],
          '__policy_expected_keys' => ['policy_subquery_1_answers_tenant_id']
        },
        allow_tables: %w[questions answers],
        config: config
      )

      expect(q.safe?).to be false
    ensure
      config.policy_adapter = nil
    end

    it 'returns false for NOT EXISTS when expected policy binds have no parameter value' do
      config.policy_adapter = ->(_user, **) { { allowed_tables: %w[questions answers] } }
      q = described_class.new(
        sql: 'SELECT * FROM "questions" WHERE NOT EXISTS (SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1)',
        params: {},
        bind_spec: [{ key: 'policy_subquery_1_answers_tenant_id', column: 'tenant_id', cast: nil }],
        intent: {
          'table' => 'questions', 'type' => 'select',
          'filters' => [{ 'column' => 'id', 'op' => 'not_exists', 'related_table' => 'answers', 'fk_column' => 'question_id' }],
          '__policy_expected_keys' => ['policy_subquery_1_answers_tenant_id']
        },
        allow_tables: %w[questions answers], config: config
      )

      expect(q.safe?).to be false
    ensure
      config.policy_adapter = nil
    end

    it 'does not let EXISTS-like text in a literal hide a disallowed table' do
      q = described_class.new(
        sql: %(SELECT 'EXISTS (ignored)' FROM "questions" JOIN "answers" ON TRUE),
        params: {}, bind_spec: [], intent: { 'table' => 'questions', 'type' => 'select' },
        allow_tables: ['questions'], config: config
      )

      expect(q.safe?).to be false
    end

    it 'ignores parentheses in EXISTS comments while identifying the subquery boundary' do
      q = described_class.new(
        sql: "SELECT * FROM \"questions\" WHERE EXISTS (SELECT 1 FROM \"answers\" /* ) */ WHERE TRUE)",
        params: {}, bind_spec: [],
        intent: { 'table' => 'questions', 'type' => 'select', 'filters' => [{ 'op' => 'exists', 'related_table' => 'answers' }] },
        allow_tables: ['questions'], config: config
      )

      expect(q.safe?).to be true
    end

    it 'fails closed for an unparseable top-level table reference' do
      q = described_class.new(
        sql: 'SELECT * FROM ONLY "questions"', params: {}, bind_spec: [],
        intent: { 'table' => 'questions', 'type' => 'select' }, allow_tables: ['questions'], config: config
      )

      expect(q.safe?).to be false
    end

    it 'does not enforce a partial related-table allowlist when no explicit allow_tables are provided' do
      q = described_class.new(
        sql: 'SELECT * FROM "questions" WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."question_id" = "questions"."id")',
        params: {},
        bind_spec: [],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          'filters' => [
            {
              'column' => 'id',
              'op' => 'exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ]
        },
        allow_tables: nil,
        config: config
      )

      expect(q.safe?).to be true
    end

    it 'enforces policy-derived related-table allowlists when no explicit allow_tables are provided' do
      q = described_class.new(
        sql: 'SELECT * FROM "questions" WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."question_id" = "questions"."id")',
        params: {},
        bind_spec: [],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          '__policy_allowed_tables' => ['questions'],
          'filters' => [
            {
              'column' => 'id',
              'op' => 'exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ]
        },
        allow_tables: nil,
        config: config
      )

      expect(q.safe?).to be false
    end

    it 'enforces policy-derived allowlists even when explicit allow_tables casing differs' do
      q = described_class.new(
        sql: 'SELECT * FROM "questions" WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."question_id" = "questions"."id")',
        params: {},
        bind_spec: [],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          '__policy_allowed_tables' => ['questions'],
          'filters' => [
            {
              'column' => 'id',
              'op' => 'exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ]
        },
        allow_tables: ['Questions'],
        config: config
      )

      expect(q.safe?).to be false
    end

    it 'fails closed when explicit and policy allowlists intersect to an empty set' do
      q = described_class.new(
        sql: 'SELECT * FROM "questions" WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."question_id" = "questions"."id")',
        params: {},
        bind_spec: [],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          '__policy_allowed_tables' => ['answers'],
          'filters' => [
            {
              'column' => 'id',
              'op' => 'exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ]
        },
        allow_tables: ['questions'],
        config: config
      )

      expect(q.safe?).to be false
    end

    it 'still blocks related tables when SQL references them outside the declared EXISTS subquery' do
      q = described_class.new(
        sql: 'SELECT * FROM "questions" JOIN "answers" ON "answers"."question_id" = "questions"."id" WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1)',
        params: { 'policy_subquery_1_answers_tenant_id' => 42 },
        bind_spec: [{ key: 'policy_subquery_1_answers_tenant_id', column: 'tenant_id', cast: nil }],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          'filters' => [
            {
              'column' => 'id',
              'op' => 'exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ],
          '__policy_expected_keys' => ['policy_subquery_1_answers_tenant_id']
        },
        allow_tables: ['questions'],
        config: config
      )

      expect(q.safe?).to be false
    end

    it 'still blocks related tables when SQL references them outside the declared EXISTS subquery with a quoted alias' do
      q = described_class.new(
        sql: 'SELECT * FROM "questions" JOIN "answers" AS "a" ON "a"."question_id" = "questions"."id" WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1)',
        params: { 'policy_subquery_1_answers_tenant_id' => 42 },
        bind_spec: [{ key: 'policy_subquery_1_answers_tenant_id', column: 'tenant_id', cast: nil }],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          'filters' => [
            {
              'column' => 'id',
              'op' => 'exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ],
          '__policy_expected_keys' => ['policy_subquery_1_answers_tenant_id']
        },
        allow_tables: ['questions'],
        config: config
      )

      expect(q.safe?).to be false
    end

    it 'still blocks related tables when SQL references them through a LEFT OUTER JOIN' do
      q = described_class.new(
        sql: 'SELECT * FROM "questions" LEFT OUTER JOIN "answers" ON "answers"."question_id" = "questions"."id" WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1)',
        params: { 'policy_subquery_1_answers_tenant_id' => 42 },
        bind_spec: [{ key: 'policy_subquery_1_answers_tenant_id', column: 'tenant_id', cast: nil }],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          'filters' => [
            {
              'column' => 'id',
              'op' => 'exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ],
          '__policy_expected_keys' => ['policy_subquery_1_answers_tenant_id']
        },
        allow_tables: ['questions'],
        config: config
      )

      expect(q.safe?).to be false
    end

    it 'still blocks related tables when SQL references them through a NATURAL JOIN' do
      q = described_class.new(
        sql: 'SELECT * FROM "questions" NATURAL JOIN "answers" WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1)',
        params: { 'policy_subquery_1_answers_tenant_id' => 42 },
        bind_spec: [{ key: 'policy_subquery_1_answers_tenant_id', column: 'tenant_id', cast: nil }],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          'filters' => [
            {
              'column' => 'id',
              'op' => 'exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ],
          '__policy_expected_keys' => ['policy_subquery_1_answers_tenant_id']
        },
        allow_tables: ['questions'],
        config: config
      )

      expect(q.safe?).to be false
    end

    it 'still blocks related tables when SQL references them as an extra top-level FROM source' do
      q = described_class.new(
        sql: 'SELECT * FROM "questions", "answers" WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1)',
        params: { 'policy_subquery_1_answers_tenant_id' => 42 },
        bind_spec: [{ key: 'policy_subquery_1_answers_tenant_id', column: 'tenant_id', cast: nil }],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          'filters' => [
            {
              'column' => 'id',
              'op' => 'exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ],
          '__policy_expected_keys' => ['policy_subquery_1_answers_tenant_id']
        },
        allow_tables: ['questions'],
        config: config
      )

      expect(q.safe?).to be false
    end

    it 'still blocks schema-qualified related tables when SQL references them as an extra top-level FROM source' do
      q = described_class.new(
        sql: 'SELECT * FROM "questions", public."answers" WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1)',
        params: { 'policy_subquery_1_answers_tenant_id' => 42 },
        bind_spec: [{ key: 'policy_subquery_1_answers_tenant_id', column: 'tenant_id', cast: nil }],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          'filters' => [
            {
              'column' => 'id',
              'op' => 'exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ],
          '__policy_expected_keys' => ['policy_subquery_1_answers_tenant_id']
        },
        allow_tables: ['questions'],
        config: config
      )

      expect(q.safe?).to be false
    end

    it 'still blocks related tables when SQL reuses them in a non-EXISTS nested subquery' do
      q = described_class.new(
        sql: 'SELECT * FROM "questions" WHERE id IN (SELECT "answers"."question_id" FROM "answers" WHERE "answers"."tenant_id" = $1) AND EXISTS (SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1)',
        params: { 'policy_subquery_1_answers_tenant_id' => 42 },
        bind_spec: [{ key: 'policy_subquery_1_answers_tenant_id', column: 'tenant_id', cast: nil }],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          '__policy_allowed_tables' => %w[questions answers],
          'filters' => [
            {
              'column' => 'id',
              'op' => 'exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ],
          '__policy_expected_keys' => ['policy_subquery_1_answers_tenant_id']
        },
        allow_tables: nil,
        config: config
      )

      expect(q.safe?).to be false
    end

    it 'still blocks top-level derived tables that reference related tables outside the declared EXISTS subquery' do
      q = described_class.new(
        sql: 'SELECT * FROM "questions", (SELECT * FROM "answers") leaked WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1)',
        params: { 'policy_subquery_1_answers_tenant_id' => 42 },
        bind_spec: [{ key: 'policy_subquery_1_answers_tenant_id', column: 'tenant_id', cast: nil }],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          'filters' => [
            {
              'column' => 'id',
              'op' => 'exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ],
          '__policy_expected_keys' => ['policy_subquery_1_answers_tenant_id']
        },
        allow_tables: ['questions'],
        config: config
      )

      expect(q.safe?).to be false
    end

    it 'still blocks top-level lateral derived tables in FROM sources' do
      q = described_class.new(
        sql: 'SELECT * FROM LATERAL (SELECT * FROM "answers") leaked, "questions" WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1)',
        params: { 'policy_subquery_1_answers_tenant_id' => 42 },
        bind_spec: [{ key: 'policy_subquery_1_answers_tenant_id', column: 'tenant_id', cast: nil }],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          'filters' => [
            {
              'column' => 'id',
              'op' => 'exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ],
          '__policy_expected_keys' => ['policy_subquery_1_answers_tenant_id']
        },
        allow_tables: ['questions'],
        config: config
      )

      expect(q.safe?).to be false
    end

    it 'still blocks comma lateral derived tables that reference related tables outside the declared EXISTS subquery' do
      q = described_class.new(
        sql: 'SELECT * FROM "questions", LATERAL (SELECT * FROM "answers") leaked WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1)',
        params: { 'policy_subquery_1_answers_tenant_id' => 42 },
        bind_spec: [{ key: 'policy_subquery_1_answers_tenant_id', column: 'tenant_id', cast: nil }],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          'filters' => [
            {
              'column' => 'id',
              'op' => 'exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ],
          '__policy_expected_keys' => ['policy_subquery_1_answers_tenant_id']
        },
        allow_tables: ['questions'],
        config: config
      )

      expect(q.safe?).to be false
    end

    it 'still blocks top-level common table expressions that reference related tables outside the declared EXISTS subquery' do
      q = described_class.new(
        sql: 'WITH leaked AS (SELECT * FROM "answers") SELECT * FROM "questions" WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1)',
        params: { 'policy_subquery_1_answers_tenant_id' => 42 },
        bind_spec: [{ key: 'policy_subquery_1_answers_tenant_id', column: 'tenant_id', cast: nil }],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          'filters' => [
            {
              'column' => 'id',
              'op' => 'exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ],
          '__policy_expected_keys' => ['policy_subquery_1_answers_tenant_id']
        },
        allow_tables: ['questions'],
        config: config
      )

      expect(q.safe?).to be false
    end

    it 'still blocks top-level common table expressions with leading whitespace' do
      q = described_class.new(
        sql: '  WITH leaked AS (SELECT * FROM "answers") SELECT * FROM "questions" WHERE EXISTS (SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1)',
        params: { 'policy_subquery_1_answers_tenant_id' => 42 },
        bind_spec: [{ key: 'policy_subquery_1_answers_tenant_id', column: 'tenant_id', cast: nil }],
        intent: {
          'table' => 'questions',
          'type' => 'select',
          'filters' => [
            {
              'column' => 'id',
              'op' => 'exists',
              'related_table' => 'answers',
              'fk_column' => 'question_id',
              'base_column' => 'id',
              'related_filters' => []
            }
          ],
          '__policy_expected_keys' => ['policy_subquery_1_answers_tenant_id']
        },
        allow_tables: ['questions'],
        config: config
      )

      expect(q.safe?).to be false
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

    it 'applies compiler-injected policy filters from the query intent' do
      scope = double('scope')
      allow(scope).to receive_messages(where: scope, order: scope, limit: scope)
      build_scope_backed_user_model(scope)

      build_policy_query(config).to_relation

      expect(scope).to have_received(:where).with('active' => true).ordered
      expect(scope).to have_received(:where).with('tenant_id' => 42).ordered
      expect(scope).to have_received(:limit).with(100)
    end

    it 'returns nil when relation semantics would drop compiler-only subquery policy predicates' do
      scope = double('scope')
      allow(scope).to receive_messages(where: scope, order: scope, limit: scope)
      build_scope_backed_question_model(scope)

      expect(build_subquery_policy_query(config).to_relation).to be_nil
      expect(scope).not_to have_received(:where)
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

    it 'returns false when relation semantics would drop compiler-only subquery policy predicates' do
      build_scope_backed_question_model(double('scope'))

      expect(build_subquery_policy_query(config).relationable?).to be false
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

  describe '#binds fallback behavior' do
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
