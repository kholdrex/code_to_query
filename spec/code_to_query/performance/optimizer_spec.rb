# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery::Performance::Optimizer do
  let(:config) { stub_config(default_limit: 100) }
  let(:optimizer) { described_class.new(config) }

  describe '#optimize_query' do
    it 'analyzes simple queries correctly' do
      sql = 'SELECT * FROM users WHERE active = true ORDER BY name'
      result = optimizer.optimize_query(sql)

      expect(result[:analysis][:estimated_complexity]).to eq(:low)
      expect(result[:analysis][:join_count]).to eq(0)
      expect(result[:analysis][:has_order_by]).to be true
      expect(result[:analysis][:table_count]).to be >= 1
    end

    it 'detects complex queries' do
      sql = <<~SQL
        SELECT u.name, o.total, p.name
        FROM users u
        JOIN orders o ON u.id = o.user_id
        JOIN products p ON o.product_id = p.id
        WHERE u.active = true AND o.total > 100
        GROUP BY u.name, p.name
        ORDER BY o.total DESC
      SQL

      result = optimizer.optimize_query(sql.strip)

      expect(%i[medium high very_high]).to include(result[:analysis][:estimated_complexity])
      expect(result[:analysis][:join_count]).to eq(2)
      expect(result[:analysis][:has_aggregations]).to be false # No SUM, COUNT, etc.
      expect(result[:analysis][:has_group_by]).to be true
      expect(result[:analysis][:has_order_by]).to be true
    end

    it 'identifies query bottlenecks' do
      sql = 'SELECT * FROM users JOIN orders ON users.id = orders.user_id'
      result = optimizer.optimize_query(sql)

      bottlenecks = result[:analysis][:potential_bottlenecks]
      expect(bottlenecks).to include(:missing_limit)
      expect(bottlenecks).to include(:wildcard_select)
    end

    it 'adds LIMIT to expensive queries without one' do
      sql = 'SELECT * FROM users JOIN orders ON users.id = orders.user_id'
      result = optimizer.optimize_query(sql)

      expect(result[:optimized_sql]).to include("LIMIT #{config.default_limit}")
    end

    it 'suggests indexes for filtered columns' do
      sql = "SELECT * FROM users WHERE email = 'test@example.com' AND age > 21 LIMIT 10"
      result = optimizer.optimize_query(sql)

      suggestions = result[:analysis][:index_recommendations]
      expect(suggestions).to be_an(Array)
      expect(suggestions.map { |s| s[:column] }).to include('email', 'age')
    end

    it 'recommends composite indexes for multiple WHERE conditions' do
      sql = "SELECT * FROM users WHERE status = 'active' AND created_at > '2023-01-01' LIMIT 10"
      result = optimizer.optimize_query(sql)

      suggestions = result[:analysis][:index_recommendations]
      composite_suggestion = suggestions.find { |s| s[:type] == :composite }
      expect(composite_suggestion).not_to be_nil
      expect(composite_suggestion[:columns]).to include('status', 'created_at')
    end

    it 'generates helpful recommendations' do
      sql = 'SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id JOIN categories c ON p.category_id = c.id'
      result = optimizer.optimize_query(sql)

      recommendations = result[:recommendations]
      expect(recommendations).to include(match(/JOIN/i))
      expect(recommendations).to include(match(/SELECT \*/))
    end
  end

  describe '#track_query_performance' do
    let(:sql) { 'SELECT * FROM users WHERE active = true LIMIT 10' }

    it 'tracks basic performance metrics' do
      stats = optimizer.track_query_performance(sql, 0.5, 25)

      expect(stats[:sql]).to eq(sql)
      expect(stats[:execution_count]).to eq(1)
      expect(stats[:total_time]).to eq(0.5)
      expect(stats[:avg_time]).to eq(0.5)
      expect(stats[:min_time]).to eq(0.5)
      expect(stats[:max_time]).to eq(0.5)
    end

    it 'accumulates statistics over multiple executions' do
      optimizer.track_query_performance(sql, 0.3, 20)
      optimizer.track_query_performance(sql, 0.7, 30)
      stats = optimizer.track_query_performance(sql, 0.5, 25)

      expect(stats[:execution_count]).to eq(3)
      expect(stats[:total_time]).to eq(1.5)
      expect(stats[:avg_time]).to eq(0.5)
      expect(stats[:min_time]).to eq(0.3)
      expect(stats[:max_time]).to eq(0.7)
    end

    it 'issues performance alerts for slow queries' do
      expect do
        optimizer.track_query_performance(sql, 2.0) # Slow query
      end.to output(/PERFORMANCE ALERT.*Slow query/).to_stderr
    end

    it 'detects performance degradation' do
      # Setup: multiple fast executions
      5.times { optimizer.track_query_performance(sql, 0.1) }

      # Then a very slow execution
      expect do
        optimizer.track_query_performance(sql, 1.5) # Much slower than average
      end.to output(/PERFORMANCE ALERT.*degrading/).to_stderr
    end
  end

  describe '#performance_report' do
    before do
      # Create some test data
      optimizer.track_query_performance('SELECT * FROM users LIMIT 10', 0.1)
      optimizer.track_query_performance('SELECT * FROM orders LIMIT 10', 0.3)
      optimizer.track_query_performance('SELECT * FROM users LIMIT 10', 0.15) # Second execution
      optimizer.track_query_performance('SELECT COUNT(*) FROM products', 2.0) # Slow query
    end

    it 'provides comprehensive performance insights' do
      report = optimizer.performance_report

      expect(report[:total_queries]).to eq(3) # Unique queries
      expect(report[:most_frequent]).to be_an(Array)
      expect(report[:slowest]).to be_an(Array)
      expect(report[:fastest]).to be_an(Array)
    end

    it 'identifies most frequent queries' do
      report = optimizer.performance_report
      most_frequent = report[:most_frequent]

      expect(most_frequent.first[:sql]).to include('users')
      expect(most_frequent.first[:executions]).to eq(2)
    end

    it 'identifies slowest queries' do
      report = optimizer.performance_report
      slowest = report[:slowest]

      expect(slowest.first[:sql]).to include('COUNT')
      expect(slowest.first[:avg_time]).to eq(2.0)
    end

    it 'identifies fastest queries with multiple executions' do
      report = optimizer.performance_report
      fastest = report[:fastest]

      # Should only include queries with multiple executions
      expect(fastest.all? { |q| q[:executions] > 1 }).to be true
    end
  end

  describe 'private helper methods' do
    describe '#estimate_complexity' do
      it 'rates simple queries as low complexity' do
        sql = 'SELECT id FROM users WHERE active = true LIMIT 10'
        complexity = optimizer.send(:estimate_complexity, sql)
        expect(complexity).to eq(:low)
      end

      it 'rates queries with joins as higher complexity' do
        sql = 'SELECT * FROM users JOIN orders ON users.id = orders.user_id LIMIT 10'
        complexity = optimizer.send(:estimate_complexity, sql)
        expect(%i[medium high]).to include(complexity)
      end

      it 'rates queries with subqueries as higher complexity' do
        sql = 'SELECT * FROM users WHERE id IN (SELECT user_id FROM orders) LIMIT 10'
        complexity = optimizer.send(:estimate_complexity, sql)
        expect(%i[medium high very_high]).to include(complexity)
      end
    end

    describe '#extract_where_columns' do
      it 'extracts column names from WHERE clauses' do
        sql = "SELECT * FROM users WHERE name = 'John' AND age > 25 AND status IN ('active', 'pending')"
        columns = optimizer.send(:extract_where_columns, sql)
        expect(columns).to include('name', 'age', 'status')
      end

      it 'handles complex WHERE clauses' do
        sql = "SELECT * FROM users WHERE (name = 'John' OR email LIKE '%@example.com') AND created_at > '2023-01-01'"
        columns = optimizer.send(:extract_where_columns, sql)
        expect(columns).to include('name', 'email', 'created_at')
      end
    end

    describe '#extract_order_columns' do
      it 'extracts column names from ORDER BY clauses' do
        sql = 'SELECT * FROM users ORDER BY name ASC, created_at DESC'
        columns = optimizer.send(:extract_order_columns, sql)
        expect(columns).to include('name', 'created_at')
      end

      it 'filters out ASC/DESC keywords' do
        sql = 'SELECT * FROM users ORDER BY name DESC'
        columns = optimizer.send(:extract_order_columns, sql)
        expect(columns).to eq(['name'])
      end
    end

    describe '#identify_bottlenecks' do
      it 'identifies missing LIMIT as bottleneck' do
        sql = 'SELECT * FROM users WHERE active = true'
        bottlenecks = optimizer.send(:identify_bottlenecks, sql)
        expect(bottlenecks).to include(:missing_limit)
      end

      it 'identifies wildcard SELECT as bottleneck' do
        sql = 'SELECT * FROM users LIMIT 10'
        bottlenecks = optimizer.send(:identify_bottlenecks, sql)
        expect(bottlenecks).to include(:wildcard_select)
      end

      it 'identifies too many JOINs as bottleneck' do
        sql = 'SELECT * FROM a JOIN b ON a.id=b.a_id JOIN c ON b.id=c.b_id JOIN d ON c.id=d.c_id JOIN e ON d.id=e.d_id LIMIT 10'
        bottlenecks = optimizer.send(:identify_bottlenecks, sql)
        expect(bottlenecks).to include(:too_many_joins)
      end
    end
  end
end
