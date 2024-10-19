# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery::Guardrails::SqlLinter do
  let(:config) { stub_config(max_limit: 1000, max_joins: 3) }
  let(:linter) { described_class.new(config, allow_tables: %w[users orders products]) }

  describe 'advanced SQL injection protection' do
    context 'with sophisticated injection attempts' do
      it 'blocks hex-encoded injection attempts' do
        sql = 'SELECT * FROM users WHERE id = 0x41424344 LIMIT 10'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /Dangerous SQL pattern detected/)
      end

      it 'blocks URL-encoded injection patterns' do
        sql = "SELECT * FROM users WHERE name = '%20OR%201=1--' LIMIT 10"
        expect { linter.check!(sql) }.to raise_error(SecurityError, /Dangerous SQL pattern detected/)
      end

      it 'blocks polyglot attacks across databases' do
        sql = 'SELECT * FROM information_schema.tables UNION SELECT version(), database() LIMIT 10'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /Dangerous SQL pattern detected/)
      end

      it 'blocks time-based blind injection' do
        sql = 'SELECT * FROM users WHERE id = 1 OR SLEEP(5) LIMIT 10'
        expect { linter.check!(sql) }.to raise_error(SecurityError)
      end

      it 'blocks comment-based injection' do
        sql = 'SELECT * FROM users WHERE id = 1 /* OR 1=1 */ LIMIT 10'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /Dangerous SQL pattern detected/)
      end

      it 'blocks encoding bypass attempts' do
        sql = "SELECT * FROM users WHERE name = '\\x41\\x42\\x43' LIMIT 10"
        expect { linter.check!(sql) }.to raise_error(SecurityError, /Potential encoding bypass detected/)
      end
    end

    context 'with function-based attacks' do
      it 'blocks dangerous system functions' do
        sql = 'SELECT extractvalue(1, concat(0x7e, version(), 0x7e)) FROM users LIMIT 10'
        expect { linter.check!(sql) }.to raise_error(SecurityError)
      end

      it 'blocks file access functions' do
        sql = 'SELECT load_file(??) FROM users LIMIT 10'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /Dangerous function.*not allowed/)
      end

      it 'blocks system command execution' do
        sql = 'SELECT sys_exec(??) FROM users LIMIT 10'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /Dangerous function.*not allowed/)
      end
    end

    context 'with advanced evasion techniques' do
      it 'blocks concatenation-based injection' do
        sql = "SELECT * FROM users WHERE name = 'test' || ' OR 1=1' LIMIT 10"
        expect { linter.check!(sql) }.to raise_error(SecurityError, /Dangerous SQL pattern detected/)
      end

      it 'blocks nested subquery injection' do
        sql = 'SELECT * FROM users WHERE id IN (SELECT id FROM users WHERE 1=1) LIMIT 10'
        # This should pass basic checks but could be flagged in advanced mode
        expect { linter.check!(sql) }.to raise_error(SecurityError) if config.block_subqueries
      end
    end
  end

  describe 'complex query validation' do
    context 'with join complexity' do
      it 'validates complex JOIN patterns with aliases' do
        sql = <<~SQL
          SELECT u.name, o.total, p.name as product_name#{' '}
          FROM users u#{' '}
          INNER JOIN orders o ON u.id = o.user_id#{' '}
          LEFT JOIN products p ON o.product_id = p.id#{' '}
          WHERE u.active = true#{' '}
          ORDER BY o.created_at DESC#{' '}
          LIMIT 50
        SQL

        expect { linter.check!(sql.strip) }.not_to raise_error
      end

      it 'blocks excessive JOIN complexity' do
        sql = <<~SQL
          SELECT * FROM users u1
          JOIN orders o1 ON u1.id = o1.user_id
          JOIN products p1 ON o1.product_id = p1.id
          JOIN categories c1 ON p1.category_id = c1.id
          JOIN suppliers s1 ON p1.supplier_id = s1.id
          LIMIT 10
        SQL

        # This exceeds max_joins (3) and should fail
        linter_strict = described_class.new(config, allow_tables: %w[users orders products categories suppliers])
        expect { linter_strict.check!(sql.strip) }.to raise_error(SecurityError, /Too many JOINs/)
      end

      it 'requires explicit ON conditions for all JOINs' do
        sql = 'SELECT * FROM users JOIN orders LIMIT 10'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /explicit ON conditions/)
      end
    end

    context 'with table name extraction' do
      it 'correctly extracts tables with various quote styles' do
        sql = 'SELECT * FROM "users" u JOIN `orders` o ON u.id = o.user_id LIMIT 10'
        extracted_tables = linter.send(:extract_table_names, sql)
        expect(extracted_tables).to contain_exactly('users', 'orders')
      end

      it 'handles table aliases correctly' do
        sql = 'SELECT * FROM users AS u JOIN orders o ON u.id = o.user_id LIMIT 10'
        extracted_tables = linter.send(:extract_table_names, sql)
        expect(extracted_tables).to contain_exactly('users', 'orders')
      end
    end
  end

  describe 'performance and safety validation' do
    context 'with LIMIT enforcement' do
      it 'enforces reasonable LIMIT values' do
        sql = 'SELECT * FROM users LIMIT 50000'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /exceeds maximum/)
      end

      it 'rejects zero or negative LIMITs' do
        sql = 'SELECT * FROM users LIMIT 0'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /must be positive/)
      end

      it 'requires LIMIT for potentially expensive queries' do
        sql = "SELECT * FROM users WHERE created_at > '2023-01-01'"
        expect { linter.check!(sql) }.to raise_error(SecurityError, /LIMIT clause is required/)
      end
    end

    context 'with literal validation' do
      it 'blocks dangerous string literals in WHERE clauses' do
        sql = "SELECT * FROM users WHERE email = 'admin@example.com' LIMIT 10"
        expect { linter.check!(sql) }.to raise_error(SecurityError, /String literals are not allowed/)
      end

      it 'blocks large numeric literals' do
        sql = 'SELECT * FROM users WHERE id = 123456789 LIMIT 10'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /Numeric literals.*should be parameterized/)
      end

      it 'allows small numeric literals' do
        sql = 'SELECT * FROM users WHERE active = 1 LIMIT 10'
        expect { linter.check!(sql) }.not_to raise_error
      end

      it 'allows parameterized LIKE patterns' do
        sql = 'SELECT * FROM users WHERE name LIKE $1 LIMIT 10'
        expect { linter.check!(sql) }.not_to raise_error
      end
    end
  end

  describe 'database-specific protection' do
    context 'with PostgreSQL-specific patterns' do
      it 'blocks PostgreSQL system function access' do
        sql = "SELECT pg_read_file('/etc/passwd') FROM users LIMIT 10"
        expect { linter.check!(sql) }.to raise_error(SecurityError)
      end

      it 'blocks PostgreSQL copy operations' do
        sql = "SELECT * FROM users; COPY users TO '/tmp/dump.txt'"
        expect { linter.check!(sql) }.to raise_error(SecurityError, /semicolons/)
      end
    end

    context 'with MySQL-specific patterns' do
      it 'blocks MySQL file operations' do
        sql = 'SELECT * FROM users INTO OUTFILE $1 LIMIT 10'
        expect { linter.check!(sql) }.to raise_error(SecurityError)
      end

      it 'blocks MySQL version-specific comments' do
        sql = 'SELECT * FROM users /*!50000 WHERE 1=1 */ LIMIT 10'
        expect { linter.check!(sql) }.to raise_error(SecurityError, /Dangerous SQL pattern detected/)
      end
    end

    context 'with SQL Server-specific patterns' do
      it 'blocks xp_cmdshell execution' do
        sql = "SELECT * FROM users; EXEC xp_cmdshell 'whoami'"
        expect { linter.check!(sql) }.to raise_error(SecurityError, /semicolons/)
      end

      it 'blocks WAITFOR delay attacks' do
        sql = "SELECT * FROM users WHERE id = 1 OR (SELECT WAITFOR DELAY '00:00:05') LIMIT 10"
        expect { linter.check!(sql) }.to raise_error(SecurityError)
      end
    end
  end

  describe 'edge cases and regression tests' do
    it 'handles empty SQL gracefully' do
      expect { linter.check!('') }.to raise_error(SecurityError, /Only SELECT statements/)
    end

    it 'handles whitespace-only SQL' do
      expect { linter.check!("   \n\t  ") }.to raise_error(SecurityError, /Only SELECT statements/)
    end

    it 'handles extremely long SQL safely' do
      long_sql = "SELECT * FROM users WHERE #{'name = ? OR 1=1 OR ' * 100}active = true LIMIT 10"
      expect { linter.check!(long_sql) }.to raise_error(SecurityError, /Dangerous SQL pattern detected/)
    end

    it 'handles Unicode and special characters safely' do
      sql = "SELECT * FROM users WHERE name = 'José Müller' LIMIT 10"
      expect { linter.check!(sql) }.to raise_error(SecurityError, /String literals/)
    end

    context 'with malformed SQL' do
      it 'handles missing closing quotes' do
        sql = "SELECT * FROM users WHERE name = 'unclosed LIMIT 10"
        # This should not crash the linter
        expect { linter.check!(sql) }.not_to raise_error
      end

      it 'handles unbalanced parentheses' do
        sql = 'SELECT * FROM users WHERE (id = 1 LIMIT 10'
        # Should not crash, basic validation should still work
        expect { linter.check!(sql) }.not_to raise_error
      end
    end
  end
end
