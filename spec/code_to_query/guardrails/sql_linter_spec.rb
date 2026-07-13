# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery::Guardrails::SqlLinter do
  let(:config) { stub_config(max_limit: 1000, max_joins: 2) }
  let(:linter) { described_class.new(config, allow_tables: %w[users orders]) }

  describe '#check!' do
    context 'with valid SELECT queries' do
      it 'passes basic SELECT with LIMIT' do
        sql = 'SELECT * FROM "users" LIMIT 100'
        expect { linter.check!(sql) }.not_to raise_error
      end

      it 'passes SELECT with WHERE and parameters' do
        sql = 'SELECT * FROM "users" WHERE "active" = $1 LIMIT 50'
        expect { linter.check!(sql) }.not_to raise_error
      end

      it 'passes an ordinary table identifier inside a parenthesized expression' do
        sql = 'SELECT (table IS NULL) FROM users LIMIT 1'

        expect { linter.check!(sql) }.not_to raise_error
      end

      it 'passes BETWEEN on an ordinary table identifier' do
        sql = 'SELECT (table BETWEEN $1 AND $2) FROM users LIMIT 1'

        expect { linter.check!(sql) }.not_to raise_error
      end

      it 'passes AT TIME ZONE on an ordinary table identifier' do
        sql = 'SELECT (table AT TIME ZONE $1) FROM users LIMIT 1'

        expect { linter.check!(sql) }.not_to raise_error
      end
    end

    context 'with dangerous queries' do
      it 'blocks non-SELECT statements' do
        sql = 'INSERT INTO users (email) VALUES ("test@example.com")'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /Only SELECT statements/)
      end

      it 'blocks queries with semicolons' do
        sql = 'SELECT * FROM users; DROP TABLE users;'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /semicolons/)
      end

      it 'blocks dangerous keywords' do
        sql = 'SELECT * FROM users; UPDATE users SET email = "hacked"'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /semicolons/)
      end

      it 'blocks string literals' do
        sql = "SELECT * FROM users WHERE email = 'test@example.com' LIMIT 100"
        expect { linter.check!(sql) }.to raise_error(SecurityError, /String literals/)
      end

      it 'blocks SQL injection patterns' do
        sql = 'SELECT * FROM users WHERE id = 1 OR 1=1 LIMIT 100'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /Dangerous SQL pattern detected/)
      end
    end

    context 'with LIMIT validation' do
      it 'requires LIMIT clause' do
        sql = 'SELECT * FROM "users"'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /LIMIT/)
      end

      it 'validates LIMIT value' do
        sql = 'SELECT * FROM "users" LIMIT 2000'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /exceeds maximum/)
      end

      it 'rejects zero or negative limits' do
        sql = 'SELECT * FROM "users" LIMIT 0'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /must be positive/)
      end
    end

    context 'with table allowlist' do
      it 'allows queries on allowlisted tables' do
        sql = 'SELECT * FROM "users" LIMIT 100'
        expect { linter.check!(sql) }.not_to raise_error
      end

      it 'blocks queries on non-allowlisted tables' do
        sql = 'SELECT * FROM "admin_secrets" LIMIT 100'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /not in the allowed list/)
      end

      it 'validates JOIN table allowlist' do
        sql = 'SELECT * FROM "users" JOIN "admin_secrets" ON users.id = admin_secrets.user_id LIMIT 100'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /not in the allowed list/)
      end

      it 'rejects unsupported PostgreSQL TABLE query expressions inside subqueries' do
        sql = 'SELECT * FROM "users" WHERE EXISTS (TABLE "admin_secrets") LIMIT 10'

        expect { linter.check!(sql) }.to raise_error(SecurityError, /TABLE query expressions are not supported/)
      end

      it 'rejects TABLE query expressions used as nested set-operation operands' do
        sql = 'SELECT * FROM "users" WHERE EXISTS (SELECT * FROM "users" UNION TABLE "admin_secrets") LIMIT 10'

        expect { linter.check!(sql) }.to raise_error(SecurityError, /TABLE query expressions are not supported/)
      end

      it 'rejects TABLE query expressions with Unicode PostgreSQL identifiers' do
        sql = 'SELECT * FROM "users" WHERE EXISTS (TABLE évil) LIMIT 10'

        expect { linter.check!(sql) }.to raise_error(SecurityError, /TABLE query expressions are not supported/)
      end

      it 'rejects TABLE query expressions with PostgreSQL Unicode-escaped quoted identifiers' do
        expressions = [
          'SELECT EXISTS (TABLE U&"secrets") LIMIT 1',
          %q(SELECT EXISTS (TABLE U&"s\0065crets") LIMIT 1),
          'SELECT EXISTS (TABLE U&"secr""ets") LIMIT 1'
        ]

        expressions.each do |sql|
          expect { linter.check!(sql) }
            .to raise_error(SecurityError, /TABLE query expressions are not supported/), sql
        end
      end

      it 'rejects nested TABLE ONLY expressions across comments and whitespace' do
        sql = "SELECT * FROM \"users\" WHERE EXISTS ((TABLE /* gap */ ONLY\n(évil))) LIMIT 10"

        expect { linter.check!(sql) }.to raise_error(SecurityError, /TABLE query expressions are not supported/)
      end

      it 'rejects a TABLE query expression after a CTE definition' do
        sql = 'SELECT * FROM "users" WHERE EXISTS (WITH q AS (SELECT 1) /* gap */ TABLE évil) LIMIT 10'

        expect { linter.check!(sql) }.to raise_error(SecurityError, /TABLE query expressions are not supported/)
      end

      it 'rejects a TABLE query expression with an emoji PostgreSQL identifier' do
        sql = 'SELECT EXISTS (WITH q AS (SELECT 1) TABLE 💣) LIMIT 1'

        expect { linter.check!(sql) }.to raise_error(SecurityError, /TABLE query expressions are not supported/)
      end

      it 'does not treat table in identifiers, strings, or comments as a TABLE query expression' do
        scanner = CodeToQuery::Query::SqlScanner.new

        expect(scanner.table_query_expression?('SELECT table FROM users')).to be false
        expect(scanner.table_query_expression?('SELECT table.id FROM users AS table')).to be false
        expect(scanner.table_query_expression?('SELECT users.table FROM users')).to be false
        expect(scanner.table_query_expression?('SELECT 1 AS table FROM users')).to be false
        expect(scanner.table_query_expression?('WITH q AS (SELECT 1) SELECT table FROM q AS table')).to be false
        expect(scanner.table_query_expression?('SELECT "table" FROM users')).to be false
        expect(scanner.table_query_expression?("SELECT 'TABLE évil' FROM users")).to be false
        expect(scanner.table_query_expression?('SELECT 1 /* TABLE évil */ FROM users')).to be false
      end

      context 'with MySQL identifiers' do
        let(:config) { stub_config(adapter: :mysql, max_limit: 1000, max_joins: 2) }

        it 'rejects an unquoted case mismatch' do
          expect { linter.check!('SELECT * FROM USERS LIMIT 100') }
            .to raise_error(SecurityError, /not in the allowed list/)
        end

        it 'rejects a quoted case mismatch' do
          expect { linter.check!('SELECT * FROM `USERS` LIMIT 100') }
            .to raise_error(SecurityError, /not in the allowed list/)
        end

        it 'allows an exact-case unquoted table' do
          expect { linter.check!('SELECT * FROM users LIMIT 100') }.not_to raise_error
        end

        it 'allows an exact-case quoted table' do
          expect { linter.check!('SELECT * FROM `users` LIMIT 100') }.not_to raise_error
        end
      end
    end

    context 'with JOIN complexity limits' do
      it 'allows reasonable number of JOINs' do
        sql = 'SELECT * FROM "users" JOIN "orders" ON users.id = orders.user_id LIMIT 100'
        expect { linter.check!(sql) }.not_to raise_error
      end

      it 'blocks too many JOINs' do
        linter_with_more_tables = described_class.new(config, allow_tables: %w[users orders payments invoices])
        sql = 'SELECT * FROM "users" JOIN "orders" ON users.id = orders.user_id JOIN "payments" ON orders.id = payments.order_id JOIN "invoices" ON payments.id = invoices.payment_id LIMIT 100'
        expect { linter_with_more_tables.check!(sql) }.to raise_error(SecurityError, /Too many JOINs/)
      end

      it 'requires explicit ON conditions for JOINs' do
        sql = 'SELECT * FROM "users" JOIN "orders" LIMIT 100'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /explicit ON conditions/)
      end
    end

    context 'with dangerous functions' do
      it 'blocks file access functions' do
        sql = 'SELECT load_file("/etc/passwd") FROM "users" LIMIT 100'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /load_file/)
      end

      it 'blocks system functions' do
        sql = 'SELECT sys_exec("rm -rf /") FROM "users" LIMIT 100'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /sys_exec/)
      end

      it 'blocks schema introspection' do
        sql = 'SELECT * FROM information_schema.tables LIMIT 100'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /information_schema/)
      end
    end
  end
end
