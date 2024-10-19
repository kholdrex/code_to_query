# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery::Performance::Cache do
  let(:cache) { described_class.new(max_size: 5) }

  describe '#get and #set' do
    it 'stores and retrieves values' do
      cache.set('key1', 'value1')
      expect(cache.get('key1')).to eq('value1')
    end

    it 'returns nil for non-existent keys' do
      expect(cache.get('nonexistent')).to be_nil
    end

    it 'executes block for cache misses' do
      result = cache.get('key1') { 'generated_value' }
      expect(result).to eq('generated_value')
      expect(cache.get('key1')).to eq('generated_value')
    end

    it 'respects TTL and expires entries' do
      cache.set('key1', 'value1', ttl: 0.1)
      expect(cache.get('key1')).to eq('value1')

      sleep(0.2)
      expect(cache.get('key1')).to be_nil
    end

    it 'handles different key types' do
      cache.set('string_key', 'value1')
      cache.set({ hash: 'key' }, 'value2')
      cache.set(%w[array key], 'value3')

      expect(cache.get('string_key')).to eq('value1')
      expect(cache.get({ hash: 'key' })).to eq('value2')
      expect(cache.get(%w[array key])).to eq('value3')
    end
  end

  describe 'cache eviction' do
    it 'evicts entries when size limit is reached' do
      6.times { |i| cache.set("key#{i}", "value#{i}") }

      # Cache should only hold 5 entries
      expect(cache.stats[:size]).to be <= 5
    end

    it 'evicts least recently used entries' do
      5.times { |i| cache.set("key#{i}", "value#{i}") }

      # Access some entries to make them more recently used
      cache.get('key3')
      cache.get('key4')

      # Add new entry to trigger eviction
      cache.set('new_key', 'new_value')

      # key3 and key4 should still be there, but earlier ones might be evicted
      expect(cache.get('key3')).to eq('value3')
      expect(cache.get('key4')).to eq('value4')
    end
  end

  describe '#stats' do
    it 'tracks hit and miss statistics' do
      cache.set('key1', 'value1')

      cache.get('key1') # hit
      cache.get('key2') # miss
      cache.get('key1') # hit
      cache.get('key3') # miss

      stats = cache.stats
      expect(stats[:hits]).to eq(2)
      expect(stats[:misses]).to eq(2)
      expect(stats[:hit_rate]).to eq(50.0)
    end

    it 'calculates memory usage estimate' do
      cache.set('small', 'x')
      cache.set('large', 'x' * 1000)

      stats = cache.stats
      expect(stats[:memory_usage]).to be > 0
    end
  end

  describe '#clear' do
    it 'removes all entries and resets stats' do
      3.times { |i| cache.set("key#{i}", "value#{i}") }
      cache.get('key1') # Generate some stats

      cache.clear

      expect(cache.stats[:size]).to eq(0)
      expect(cache.stats[:hits]).to eq(0)
      expect(cache.stats[:misses]).to eq(0)
    end
  end

  describe '#delete' do
    it 'removes specific entries' do
      cache.set('key1', 'value1')
      cache.set('key2', 'value2')

      cache.delete('key1')

      expect(cache.get('key1')).to be_nil
      expect(cache.get('key2')).to eq('value2')
    end
  end
end

RSpec.describe CodeToQuery::Performance::IntentCache do
  let(:intent_cache) { described_class.new }

  describe 'intent-specific caching' do
    let(:prompt) { 'Get top 10 users by creation date' }
    let(:schema_hash) { { tables: ['users'] } }
    let(:allow_tables) { %w[users orders] }
    let(:intent) { { 'type' => 'select', 'table' => 'users', 'limit' => 10 } }

    it 'caches and retrieves intents' do
      intent_cache.cache_intent(prompt, schema_hash, allow_tables, intent)

      cached = intent_cache.get_cached_intent(prompt, schema_hash, allow_tables)
      expect(cached).to eq(intent)
    end

    it 'is case-insensitive for prompts' do
      intent_cache.cache_intent(prompt.upcase, schema_hash, allow_tables, intent)

      cached = intent_cache.get_cached_intent(prompt.downcase, schema_hash, allow_tables)
      expect(cached).to eq(intent)
    end

    it 'considers schema and tables in cache key' do
      intent_cache.cache_intent(prompt, schema_hash, allow_tables, intent)

      # Different schema should miss
      different_schema = { tables: ['products'] }
      cached = intent_cache.get_cached_intent(prompt, different_schema, allow_tables)
      expect(cached).to be_nil

      # Different tables should miss
      different_tables = ['products']
      cached = intent_cache.get_cached_intent(prompt, schema_hash, different_tables)
      expect(cached).to be_nil
    end

    it 'normalizes table arrays for consistent caching' do
      intent_cache.cache_intent(prompt, schema_hash, %w[users orders], intent)

      # Different order should still hit
      cached = intent_cache.get_cached_intent(prompt, schema_hash, %w[orders users])
      expect(cached).to eq(intent)
    end
  end
end

RSpec.describe CodeToQuery::Performance::QueryCache do
  let(:query_cache) { described_class.new }

  describe 'query result caching' do
    let(:sql) { 'SELECT * FROM users WHERE active = $1 LIMIT 10' }
    let(:params) { { active: true } }
    let(:result) { [{ id: 1, name: 'John' }, { id: 2, name: 'Jane' }] }

    it 'caches and retrieves query results' do
      query_cache.cache_query_result(sql, params, result)

      cached = query_cache.get_cached_result(sql, params)
      expect(cached).to eq(result)
    end

    it 'considers parameter order in cache key' do
      query_cache.cache_query_result(sql, { a: 1, b: 2 }, result)

      # Different order should still hit
      cached = query_cache.get_cached_result(sql, { b: 2, a: 1 })
      expect(cached).to eq(result)
    end

    it 'misses cache for different parameters' do
      query_cache.cache_query_result(sql, { active: true }, result)

      cached = query_cache.get_cached_result(sql, { active: false })
      expect(cached).to be_nil
    end

    it 'normalizes SQL whitespace' do
      query_cache.cache_query_result('  SELECT * FROM users  ', params, result)

      cached = query_cache.get_cached_result('SELECT * FROM users', params)
      expect(cached).to eq(result)
    end
  end
end
