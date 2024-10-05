# frozen_string_literal: true

require 'digest'

module CodeToQuery
  module Performance
    # Intelligent caching system for query results and parsed intents
    class Cache
      DEFAULT_TTL = 3600 # 1 hour
      MAX_CACHE_SIZE = 1000

      def initialize(config = {})
        @config = config
        @cache_store = build_cache_store
        @hit_count = 0
        @miss_count = 0
        @size_limit = config[:max_size] || MAX_CACHE_SIZE
      end

      def get(key, &block)
        cache_key = normalize_key(key)

        if (cached_value = @cache_store[cache_key])
          @hit_count += 1
          # update access metadata for LRU
          if cached_value.is_a?(Hash)
            cached_value[:access_count] = (cached_value[:access_count] || 0) + 1
            cached_value[:last_access_at] = Time.now
          end
          return cached_value[:data] if cached_value[:expires_at] > Time.now

          # Expired entry
          @cache_store.delete(cache_key)
        end

        @miss_count += 1

        return nil unless block_given?

        # Generate new value
        value = block.call
        set(key, value)
        value
      end

      def set(key, value, ttl: DEFAULT_TTL)
        cache_key = normalize_key(key)

        # Evict if at size limit
        evict_if_needed

        @cache_store[cache_key] = {
          data: value,
          created_at: Time.now,
          expires_at: Time.now + ttl,
          access_count: 0
        }

        value
      end

      def delete(key)
        cache_key = normalize_key(key)
        @cache_store.delete(cache_key)
      end

      def clear
        @cache_store.clear
        @hit_count = 0
        @miss_count = 0
      end

      def stats
        {
          size: @cache_store.size,
          hits: @hit_count,
          misses: @miss_count,
          hit_rate: hit_rate,
          memory_usage: calculate_memory_usage
        }
      end

      def hit_rate
        total_requests = @hit_count + @miss_count
        return 0.0 if total_requests.zero?

        (@hit_count.to_f / total_requests * 100).round(2)
      end

      private

      def build_cache_store
        if defined?(Rails) && Rails.cache
          # Use Rails cache if available
          RailsCacheAdapter.new(Rails.cache)
        else
          # Fallback to in-memory hash
          {}
        end
      end

      def normalize_key(key)
        case key
        when String
          Digest::SHA256.hexdigest(key)
        when Hash
          Digest::SHA256.hexdigest(key.to_json)
        when Array
          Digest::SHA256.hexdigest(key.join('|'))
        else
          Digest::SHA256.hexdigest(key.to_s)
        end
      end

      def evict_if_needed
        return if @cache_store.size < @size_limit

        # LRU eviction - remove least recently used entries
        return unless @cache_store.respond_to?(:each)

        sorted_entries = @cache_store.to_a.sort_by do |_key, value|
          [value[:access_count] || 0, value[:last_access_at] || Time.at(0)]
        end

        # Remove 20% of entries
        evict_count = (@size_limit * 0.2).to_i
        evict_count.times do
          key_to_remove, _value = sorted_entries.shift
          @cache_store.delete(key_to_remove) if key_to_remove
        end
      end

      def calculate_memory_usage
        # Rough estimate of memory usage
        if @cache_store.respond_to?(:each)
          total_size = 0
          @cache_store.each do |key, value|
            total_size += key.bytesize if key.respond_to?(:bytesize)
            total_size += estimate_object_size(value[:data])
          end
          total_size
        else
          0
        end
      end

      def estimate_object_size(obj)
        case obj
        when String
          obj.bytesize
        when Hash
          obj.to_json.bytesize
        when Array
          obj.map { |item| estimate_object_size(item) }.sum
        else
          obj.to_s.bytesize
        end
      rescue StandardError
        100 # Fallback estimate
      end

      # Adapter for Rails cache
      class RailsCacheAdapter
        def initialize(rails_cache)
          @rails_cache = rails_cache
        end

        def [](key)
          @rails_cache.read(key)
        end

        def []=(key, value)
          @rails_cache.write(key, value, expires_in: 3600) # 1 hour
        end

        def delete(key)
          @rails_cache.delete(key)
        end

        def clear
          @rails_cache.clear
        end

        def size
          # Rails cache doesn't expose size easily
          0
        end

        def each(&block)
          # Rails cache doesn't support iteration
          # This limits our LRU eviction capability with Rails cache
        end
      end
    end

    # Intent parsing cache specifically for NLP results
    class IntentCache < Cache
      def initialize(config = {})
        super(config.merge(max_size: 500)) # Smaller cache for intents
      end

      def cache_intent(prompt, schema_hash, allow_tables, intent)
        cache_key = build_intent_key(prompt, schema_hash, allow_tables)
        set(cache_key, intent, ttl: 1800) # 30 minutes
      end

      def get_cached_intent(prompt, schema_hash, allow_tables)
        cache_key = build_intent_key(prompt, schema_hash, allow_tables)
        get(cache_key)
      end

      private

      def build_intent_key(prompt, schema_hash, allow_tables)
        {
          prompt: prompt.to_s.strip.downcase,
          schema: schema_hash,
          tables: Array(allow_tables).sort
        }
      end
    end

    # Query execution result cache
    class QueryCache < Cache
      def initialize(config = {})
        super(config.merge(max_size: 200)) # Smaller cache for query results
      end

      def cache_query_result(sql, params, result)
        cache_key = build_query_key(sql, params)
        # Shorter TTL for query results as data changes frequently
        set(cache_key, result, ttl: 300) # 5 minutes
      end

      def get_cached_result(sql, params)
        cache_key = build_query_key(sql, params)
        get(cache_key)
      end

      private

      def build_query_key(sql, params)
        {
          sql: sql.to_s.strip,
          params: params.sort.to_h
        }
      end
    end
  end
end
