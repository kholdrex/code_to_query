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

  describe '#policy_predicate_bind_numbers' do
    it 'accepts binds only in the expected qualified predicate' do
      expect(scanner.policy_predicate_bind_numbers('SELECT $1 FROM "answers" WHERE TRUE', 'answers', 'tenant_id')).to eq([])
      expect(scanner.policy_predicate_bind_numbers('SELECT "answers"."tenant_id" = $1 FROM "answers" WHERE TRUE', 'answers', 'tenant_id')).to eq([])
      expect(scanner.policy_predicate_bind_numbers('SELECT 1 FROM "answers" WHERE $1 = $1', 'answers', 'tenant_id')).to eq([])
      expect(scanner.policy_predicate_bind_numbers('SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1 OR TRUE', 'answers', 'tenant_id')).to eq([])
      expect(scanner.policy_predicate_bind_numbers('SELECT 1 FROM "answers" WHERE "answers"."tenant_id" = $1', 'answers', 'tenant_id')).to eq([1])
    end
  end

  describe '#extract_table_names' do
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
