# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery::Query::SqlScanner do
  subject(:scanner) { described_class.new }

  describe '#bind_placeholder_positions' do
    it 'ignores placeholders in untagged PostgreSQL dollar-quoted literals' do
      sql = 'SELECT $$spoof $1$$, $2'

      expect(scanner.bind_placeholder_positions(sql)).to eq([[sql.index('$2'), 2]])
    end

    it 'ignores placeholders in tagged PostgreSQL dollar-quoted literals' do
      sql = 'SELECT $body$spoof $1 and ?$body$, $2'

      expect(scanner.bind_placeholder_positions(sql)).to eq([[sql.index('$2'), 2]])
    end

    it 'preserves placeholder offsets after dollar-quoted literals' do
      sql = 'SELECT $tag$($9) and $other$ text$tag$, $1'

      expect(scanner.bind_placeholder_positions(sql)).to eq([[sql.index('$1'), 1]])
    end

    it 'fails closed for unterminated dollar-quoted literals' do
      expect { scanner.bind_placeholder_positions('SELECT $tag$spoof $1') }
        .to raise_error(SecurityError, 'Unterminated SQL literal or comment')
    end

    it 'ignores question marks in quoted SQLite and MySQL identifiers' do
      sql = 'SELECT "sqlite?name", `mysql?name`, ?'

      expect(scanner.bind_placeholder_positions(sql)).to eq([[sql.rindex('?'), 1]])
    end

    it 'honors backslash-escaped apostrophes in PostgreSQL E strings' do
      %w[E e].each do |prefix|
        sql = "SELECT #{prefix}'prefix\\' query_to_xml($1) suffix', $2"

        expect(scanner.bind_placeholder_positions(sql)).to eq([[sql.index('$2'), 2]])
      end
    end

    it 'distinguishes ordinary strings from PostgreSQL E strings' do
      sql = %q(SELECT 'ordinary\' , $1)

      expect(scanner.bind_placeholder_positions(sql)).to eq([[sql.index('$1'), 1]])
    end
  end

  describe '#mask_literals_comments_and_identifier_contents' do
    it 'masks denied-looking text in literals and quoted identifier bodies' do
      sql = "SELECT E'prefix\\' query_to_xml($1)', \"query_to_xml ( suffix\", " \
            'U&"query_to_xml ( decoy", query_to_xml($2)'
      masked = scanner.mask_literals_comments_and_identifier_contents(sql)

      expect(masked.scan('query_to_xml').length).to eq(1)
      expect(masked.index('query_to_xml')).to eq(sql.rindex('query_to_xml'))
    end
  end

  describe '#exists_subqueries' do
    it 'ignores parentheses inside dollar-quoted literals when finding the boundary' do
      sql = 'SELECT 1 WHERE EXISTS (SELECT $$)$$ FROM "answers")'

      expect(scanner.exists_subqueries(sql).first[:sql]).to eq('SELECT $$)$$ FROM "answers"')
    end

    it 'does not let EXISTS text in a quoted identifier swallow a real subquery' do
      sql = 'SELECT 1 AS "x EXISTS (" WHERE EXISTS (SELECT 1 FROM "answers")'

      expect(scanner.exists_subqueries(sql).map { |entry| entry[:sql] })
        .to eq(['SELECT 1 FROM "answers"'])
      expect(scanner.strip_exists_subqueries(sql)).to eq('SELECT 1 AS "x EXISTS (" WHERE TRUE')
    end
  end

  describe '#table_query_expression?' do
    it 'does not mistake an ordinary table identifier in an expression for a TABLE query' do
      expect(scanner.table_query_expression?('SELECT (table IS NULL) FROM users LIMIT 1')).to be false
      expect(scanner.table_query_expression?('SELECT (table = $1) FROM users LIMIT 1')).to be false
      expect(scanner.table_query_expression?('SELECT (table NOT IN ($1)), table.id FROM users AS table LIMIT 1')).to be false
      expect(scanner.table_query_expression?('SELECT table($1) FROM users LIMIT 1')).to be false
    end

    it 'uses the relation operand boundary to reject expression operators' do
      expressions = [
        'table BETWEEN $1 AND $2',
        'table AT TIME ZONE $1',
        'table IS DISTINCT FROM $1',
        'table = $1',
        'table <> $1',
        'table IN ($1)',
        'table NOT IN ($1)',
        'table + $1',
        'table::text'
      ]

      expressions.each do |expression|
        expect(scanner.table_query_expression?("SELECT (#{expression}) FROM users LIMIT 1")).to be(false), expression
      end
    end

    it 'still recognizes relation identifiers in TABLE query expressions' do
      expect(scanner.table_query_expression?('SELECT EXISTS (TABLE "IS")')).to be true
      expect(scanner.table_query_expression?('SELECT EXISTS (TABLE /* gap */ ONLY (💣))')).to be true
      expect(scanner.table_query_expression?('SELECT EXISTS (WITH q AS (SELECT 1) TABLE évil)')).to be true
      expect(scanner.table_query_expression?('SELECT EXISTS (SELECT 1 UNION ALL TABLE schema_name.relation_name)')).to be true
    end

    it 'recognizes PostgreSQL Unicode-escaped quoted relation identifiers' do
      expressions = [
        'SELECT EXISTS (TABLE U&"secrets") LIMIT 1',
        %q(SELECT EXISTS (TABLE U&"s\0065crets") LIMIT 1),
        'SELECT EXISTS (TABLE U&"schema".U&"secr""ets") LIMIT 1',
        %q(SELECT EXISTS (TABLE U&"s!0065crets" UESCAPE '!') LIMIT 1)
      ]

      expressions.each do |expression|
        expect(scanner.table_query_expression?(expression)).to be(true), expression
      end
    end

    it 'does not combine spaced U, ampersand, and quoted identifier tokens' do
      expect(scanner.table_query_expression?('SELECT (table U & "secrets") FROM users LIMIT 1')).to be false
      expect(scanner.table_query_expression?('SELECT (table U& "secrets") FROM users LIMIT 1')).to be false
    end

    it 'recognizes valid TABLE relation completion and query-level continuation' do
      expressions = [
        'TABLE accounts',
        'TABLE ONLY (archive.accounts) *',
        '(TABLE accounts)',
        'TABLE accounts UNION SELECT * FROM users',
        'TABLE accounts INTERSECT ALL TABLE active_accounts',
        'TABLE accounts EXCEPT TABLE archived_accounts',
        'TABLE accounts ORDER BY id',
        'TABLE accounts LIMIT 1',
        'TABLE accounts OFFSET 1',
        'TABLE accounts FETCH FIRST 1 ROW ONLY',
        'TABLE accounts FOR UPDATE'
      ]

      expressions.each do |expression|
        expect(scanner.table_query_expression?(expression)).to be(true), expression
      end
    end
  end

  describe '#policy_predicate_bind_numbers' do
    it 'derives SQLite and MySQL question-mark bind identity from the global SQL position' do
      full_sql = 'SELECT ? FROM questions WHERE EXISTS ' \
                 '(SELECT ? FROM answers WHERE answers.kind = ? AND answers.tenant_id = ?)'
      body_start = full_sql.index('SELECT ?', full_sql.index('EXISTS'))
      body = full_sql[body_start...full_sql.rindex(')')]

      expect(scanner.policy_predicate_bind_numbers(
               body, 'answers', 'tenant_id',
               question_bind_number: 3, source_sql: full_sql, source_offset: body_start
             )).to eq([])
      expect(scanner.policy_predicate_bind_numbers(
               body, 'answers', 'tenant_id',
               question_bind_number: 4, source_sql: full_sql, source_offset: body_start
             )).to eq([4])
    end

    it 'ignores question marks in literals, comments, and dollar quotes when deriving identity' do
      full_sql = "SELECT '?', $$?$$, ? /* ? */ WHERE EXISTS " \
                 '(SELECT 1 FROM answers WHERE answers.tenant_id = ?)'
      body_start = full_sql.index('SELECT 1')
      body = full_sql[body_start...full_sql.rindex(')')]

      expect(scanner.policy_predicate_bind_numbers(
               body, 'answers', 'tenant_id',
               question_bind_number: 2, source_sql: full_sql, source_offset: body_start
             )).to eq([2])
    end

    it 'accepts binds only in the expected qualified predicate' do
      expect(scanner.policy_predicate_bind_numbers('SELECT $1 FROM "answers" WHERE TRUE', 'answers', 'tenant_id')).to eq([])
      expect(scanner.policy_predicate_bind_numbers('SELECT "answers"."tenant_id" = $1 FROM "answers" WHERE TRUE', 'answers', 'tenant_id')).to eq([])
      expect(scanner.policy_predicate_bind_numbers('SELECT 1 FROM "answers" WHERE $1 = $1', 'answers', 'tenant_id')).to eq([])
      expect(scanner.policy_predicate_bind_numbers('SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1 OR TRUE', 'answers', 'tenant_id')).to eq([])
      expect(scanner.policy_predicate_bind_numbers('SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1', 'answers', 'tenant_id')).to eq([1])
    end

    it 'rejects predicates in non-enforcing boolean and expression contexts' do
      expect(scanner.policy_predicate_bind_numbers(
               'SELECT 1 FROM "answers" WHERE NOT ("answers"."tenant_id" = $1)', 'answers', 'tenant_id'
             )).to eq([])
      expect(scanner.policy_predicate_bind_numbers(
               'SELECT 1 FROM "answers" WHERE CASE WHEN "answers"."tenant_id" = $1 THEN TRUE ELSE TRUE END',
               'answers', 'tenant_id'
             )).to eq([])
    end

    it 'requires identifier boundaries for unquoted policy columns' do
      expect(scanner.policy_predicate_bind_numbers(
               'SELECT 1 FROM answers WHERE notanswers.tenant_id = $1', 'answers', 'tenant_id'
             )).to eq([])
      expect(scanner.policy_predicate_bind_numbers(
               'SELECT 1 FROM answers WHERE answers.tenant_id_suffix = $1', 'answers', 'tenant_id'
             )).to eq([])
    end

    it 'requires Unicode identifier boundaries for unquoted policy identifiers' do
      expect(scanner.policy_predicate_bind_numbers(
               'SELECT 1 FROM answers WHERE éanswers.tenant_id = $1', 'answers', 'tenant_id'
             )).to eq([])
      expect(scanner.policy_predicate_bind_numbers(
               'SELECT 1 FROM answers WHERE answersé.tenant_id = $1', 'answers', 'tenant_id'
             )).to eq([])
      expect(scanner.policy_predicate_bind_numbers(
               'SELECT 1 FROM answers WHERE answers.étenant_id = $1', 'answers', 'tenant_id'
             )).to eq([])
      expect(scanner.policy_predicate_bind_numbers(
               'SELECT 1 FROM answers WHERE answers.tenant_idé = $1', 'answers', 'tenant_id'
             )).to eq([])
    end

    it 'does not count a predicate placed in a nested scope' do
      sql = 'SELECT 1 FROM "answers" WHERE EXISTS ' \
            '(SELECT 1 FROM "decoys" WHERE "answers"."tenant_id" = $1)'

      expect(scanner.policy_predicate_bind_numbers(sql, 'answers', 'tenant_id')).to eq([])
    end

    it 'does not Unicode-case-fold policy identifiers on case-insensitive adapters' do
      %i[sqlite mysql].each do |adapter|
        sql = 'SELECT 1 FROM kids WHERE kids.Kind = $1'

        expect(scanner.policy_predicate_bind_numbers(sql, 'kids', 'kind', adapter: adapter)).to eq([])
      end
    end

    it 'limits PostgreSQL unquoted policy identifier folding to ASCII' do
      sql = 'SELECT 1 FROM kids WHERE KIDS.KIND = $1'

      expect(scanner.policy_predicate_bind_numbers(sql, 'kids', 'kind', adapter: :postgres)).to eq([])
      expect(scanner.policy_predicate_bind_numbers(
               'SELECT 1 FROM kids WHERE KIDS.KIND = $1', 'kids', 'kind', adapter: :postgres
             )).to eq([1])
    end

    it 'accepts compiler-shaped top-level conjuncts including BETWEEN' do
      sql = 'SELECT 1 FROM answers WHERE answers.question_id = questions.id ' \
            'AND answers.created_at BETWEEN $2 AND $3'

      expect(scanner.policy_predicate_bind_numbers(sql, 'answers', 'created_at')).to eq([2, 3])
    end
  end

  describe '#extract_table_names' do
    it 'fails closed for qualified table references instead of discarding the qualifier' do
      expect { scanner.extract_table_names('SELECT * FROM evil.users') }
        .to raise_error(SecurityError, /Unsupported FROM reference/)
    end

    it 'does not let identifier quotes inside string literals hide executable table references' do
      expect(scanner.extract_table_names(%q(SELECT '"' FROM"forbidden"))).to eq(['forbidden'])
      expect(scanner.extract_table_names("SELECT '`' FROM`forbidden`")).to eq(['forbidden'])
    end

    it 'does not let a string quote inside a quoted identifier hide a later table reference' do
      expect(scanner.extract_table_names(%q(SELECT 1 AS "a'b" FROM "forbidden"))).to eq(['forbidden'])
    end

    it 'masks doubled delimiters inside a quoted identifier without hiding a later table reference' do
      expect(scanner.extract_table_names('SELECT 1 AS "a""b" FROM "forbidden"')).to eq(['forbidden'])
    end

    it 'does not let identifier quotes inside dollar-quoted literals hide executable table references' do
      sql = 'SELECT $body$`$body$ FROM "allowed" JOIN"forbidden" ON TRUE'

      expect(scanner.extract_table_names(sql)).to eq(%w[allowed forbidden])
    end

    it 'extracts quoted FROM and JOIN identifiers without intervening whitespace' do
      sql = 'SELECT * FROM"questions" JOIN"answers" ON "answers"."question_id" = "questions"."id"'

      expect(scanner.extract_table_names(sql)).to eq(%w[questions answers])
    end

    it 'does not treat keyword text inside quoted identifiers as SQL structure' do
      sql = 'SELECT * FROM "questions" AS "JOIN decoy" JOIN"answers WHERE decoy" ON TRUE'

      expect(scanner.extract_table_names(sql)).to eq(['questions', 'answers WHERE decoy'])
    end

    it 'does not split a FROM reference on a comma inside a quoted identifier' do
      expect(scanner.extract_table_names('SELECT * FROM "questions, archived"')).to eq(['questions, archived'])
    end
  end
end
