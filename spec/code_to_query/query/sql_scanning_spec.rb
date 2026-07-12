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
  end

  describe '#extract_table_names' do
    it 'extracts quoted FROM and JOIN identifiers without intervening whitespace' do
      sql = 'SELECT * FROM"questions" JOIN"answers" ON "answers"."question_id" = "questions"."id"'

      expect(scanner.extract_table_names(sql)).to eq(%w[questions answers])
    end

    it 'does not treat keyword text inside quoted identifiers as SQL structure' do
      sql = 'SELECT * FROM "questions" AS "JOIN decoy" JOIN"answers WHERE decoy" ON TRUE'

      expect(scanner.extract_table_names(sql)).to eq(['questions', 'answers WHERE decoy'])
    end
  end
end
