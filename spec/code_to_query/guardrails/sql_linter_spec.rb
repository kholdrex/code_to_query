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
