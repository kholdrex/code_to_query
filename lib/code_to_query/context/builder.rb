# frozen_string_literal: true

# Context::Builder inspects the DB and app to produce a context pack file.

require 'fileutils'

begin
  require 'active_record'
rescue LoadError
end

module CodeToQuery
  module Context
    # rubocop:disable Metrics/ClassLength
    class Builder
      def self.bootstrap!
        new.bootstrap!
      end

      def initialize(config = CodeToQuery.config)
        @config = config
      end

      # Build a full pack and write it to disk
      def bootstrap!
        # First attempt to extract schema
        schema_data = extract_schema
        initial_count = schema_data[:tables]&.length || schema_data['tables']&.length || 0
        @config.logger.info("[code_to_query] Schema data structure: #{schema_data.keys} with #{initial_count} tables")

        # If schema looks empty, try scanning the app to force-load models/connection, then retry
        models_data = scan_app
        # Optionally enrich with static scan
        if @config.prefer_static_scan
          static_data = static_scan_app
          models_data = deep_merge_models(models_data, static_data)
        end
        if initial_count.to_i.zero?
          schema_data = extract_schema
          retry_count = schema_data[:tables]&.length || schema_data['tables']&.length || 0
          @config.logger.info("[code_to_query] Retried schema extraction after app scan: #{retry_count} tables")
        end

        pack = Pack.new(
          schema: schema_data,
          models: models_data,
          glossary: enrich_glossary_with_llm(generate_glossary(schema_data), schema_data, models_data),
          policies: collect_policies,
          hints: { performance: [], joins: extract_join_hints(schema_data) }
        )
        write_pack(pack)
        pack
      end

      # --- Components (stubs that won't crash) ---

      def extract_schema
        unless defined?(ActiveRecord::Base)
          return { tables: [], version: 'unknown', adapter: 'none' }
        end

        # Try multiple approaches to establish connection
        connection = nil
        connection_attempts = 0
        max_attempts = 3

        while connection.nil? && connection_attempts < max_attempts
          connection_attempts += 1
          begin
            # Force-establish a connection (Rails defers until first use)
            ActiveRecord::Base.connection

            # Verify connection is actually working
            if ActiveRecord::Base.connected?
              connection = ActiveRecord::Base.connection
              @config.logger.info("[code_to_query] Connected to database with adapter: #{connection.adapter_name} (attempt #{connection_attempts})")
              break
            else
              @config.logger.info("[code_to_query] Database not connected on attempt #{connection_attempts}")
              sleep(0.1) if connection_attempts < max_attempts
            end
          rescue StandardError => e
            @config.logger.warn("[code_to_query] Connection attempt #{connection_attempts} failed: #{e.message}")
            sleep(0.1) if connection_attempts < max_attempts
          end
        end

        unless connection
          error_msg = "Failed to establish database connection after #{max_attempts} attempts"
          @config.logger.warn("[code_to_query] #{error_msg}")
          return { tables: [], version: 'unknown', adapter: @config.adapter.to_s, error: error_msg }
        end

        tables = list_tables(connection)
        @config.logger.info("[code_to_query] Found #{tables.length} tables: #{tables.join(', ')}") if tables.any?
        @config.logger.info('[code_to_query] No tables found') if tables.empty?

        result = {
          tables: tables.map do |table_name|
            {
              name: table_name,
              columns: extract_table_columns(connection, table_name),
              indexes: extract_table_indexes(connection, table_name),
              foreign_keys: extract_foreign_keys(connection, table_name),
              constraints: extract_table_constraints(connection, table_name)
            }
          end,
          version: extract_schema_version(connection),
          adapter: connection.adapter_name.downcase
        }

        @config.logger.info("[code_to_query] Schema extraction completed with #{result[:tables].length} tables")
        result
      rescue StandardError => e
        @config.logger.warn("[code_to_query] Schema extraction failed: #{e.message}")
        @config.logger.warn("[code_to_query] Backtrace: #{e.backtrace.first(5).join("\n")}")
        { tables: [], version: 'unknown', adapter: @config.adapter.to_s, error: e.message }
      end

      def scan_app
        models = {}
        associations = {}
        validations = {}
        scopes = {}

        if defined?(ActiveRecord::Base)
          # Ensure models are loaded so descendants is populated
          if defined?(Rails) && Rails.respond_to?(:application)
            begin
              Rails.application.eager_load!
            rescue StandardError => e
              @config.logger.warn("[code_to_query] Eager load failed: #{e.message}")
            end
          end
          ActiveRecord::Base.descendants.each do |model|
            next unless model.table_exists?

            model_name = model.name
            table_name = model.table_name

            models[model_name] = {
              table_name: table_name,
              primary_key: model.primary_key,
              inheritance_column: model.inheritance_column,
              timestamps: has_timestamps?(model),
              soft_delete: has_soft_delete?(model),
              enums: extract_model_enums(model)
            }

            associations[model_name] = extract_model_associations(model)
            validations[model_name] = extract_model_validations(model)
            scopes[model_name] = extract_model_scopes(model)
          end
        end

        {
          models: models,
          associations: associations,
          validations: validations,
          scopes: scopes
        }
      rescue StandardError => e
        @config.logger.warn("[code_to_query] App scanning failed: #{e.message}")
        { models: {}, associations: {}, validations: {}, scopes: {}, error: e.message }
      end

      def generate_glossary(existing_schema = nil)
        glossary = {}

        # Auto-generate from schema (prefer the already extracted schema)
        schema = existing_schema || extract_schema
        tables = schema[:tables] || schema['tables'] || []
        tables.each do |table|
          table_name = table[:name] || table['name']

          # Generate table synonyms
          synonyms = generate_table_synonyms(table_name)
          glossary[table_name] = synonyms if synonyms.any?

          # Generate column synonyms
          Array(table[:columns] || table['columns']).each do |column|
            column_name = column[:name] || column['name']
            sql_type = column[:sql_type] || column['sql_type']
            column_synonyms = generate_column_synonyms(column_name, sql_type)
            if column_synonyms.any?
              key = "#{table_name}.#{column_name}"
              glossary[key] = column_synonyms
            end
          end
        end

        # Add business-specific glossary
        glossary.merge!(load_business_glossary)

        glossary
      rescue StandardError => e
        @config.logger.warn("[code_to_query] Glossary generation failed: #{e.message}")
        { error: e.message }
      end

      def collect_policies
        policies = {
          enforced_predicates: {},
          column_access: {},
          row_level_security: {},
          audit_requirements: {}
        }

        # Get policies from configuration
        if @config.policy_adapter.respond_to?(:call)
          begin
            # In a real implementation, you'd pass the actual user context
            user_policies = @config.policy_adapter.call(nil)
            policies[:enforced_predicates] = user_policies if user_policies.is_a?(Hash)
          rescue StandardError => e
            @config.logger.warn("[code_to_query] Policy collection failed: #{e.message}")
          end
        end

        # Extract policies from models (if using Pundit, CanCan, etc.)
        policies.merge!(extract_authorization_policies)

        policies
      rescue StandardError => e
        @config.logger.warn("[code_to_query] Policy collection failed: #{e.message}")
        { enforced_predicates: {}, error: e.message }
      end

      def verify!
        path = @config.context_pack_path.to_s
        raise "Context pack not found at #{path}" unless File.exist?(path)

        json = JSON.parse(File.read(path))
        raise 'Context pack missing schema.tables' unless json.dig('schema', 'tables').is_a?(Array)

        true
      end

      private

      # Lightweight static scan using regex heuristics to avoid runtime execution.
      # If parser/rubocop-ast is available, we could replace these regexes with AST parsing.
      def static_scan_app
        result = { models: {}, associations: {}, validations: {}, scopes: {} }
        dirs = Array(@config.static_scan_dirs).compact
        dirs.each do |dir|
          next unless Dir.exist?(dir)

          Dir.glob(File.join(dir, '**/*.rb')).each do |file|
            begin
              content = File.read(file)
            rescue StandardError
              next
            end
            model_name = infer_model_name_from_path(file)
            next unless model_name

            result[:models][model_name] ||= { table_name: nil, primary_key: 'id', inheritance_column: 'type', timestamps: true, soft_delete: false, enums: {} }
            result[:scopes][model_name] ||= {}

            # enum lines: enum attachment_type: { video: 0, image: 1 }
            content.scan(/\benum\s+([a-zA-Z0-9_]+):\s*\{([^}]+)\}/).each do |(col, body)|
              mapping = {}
              body.split(',').each do |pair|
                if (m = pair.strip.match(/([a-zA-Z0-9_]+):\s*(\d+)/))
                  mapping[m[1]] = m[2].to_i
                end
              end
              next if mapping.empty?

              result[:models][model_name][:enums][col] ||= {}
              result[:models][model_name][:enums][col].merge!(mapping)
            end

            # constant maps: ATTACHMENT_TYPES = { video: 0, image: 1 }
            content.scan(/([A-Z][A-Z0-9_]+)\s*=\s*\{([^}]+)\}/).each do |(const_name, body)|
              mapping = {}
              body.split(',').each do |pair|
                if (m = pair.strip.match(/([a-zA-Z0-9_]+):\s*(\d+)/))
                  mapping[m[1]] = m[2].to_i
                end
              end
              next if mapping.empty?

              base = const_name.downcase.sub(/_types\z/, '').sub(/_type\z/, '').sub(/_statuses\z/, '').sub(/_status\z/, '').sub(/_kinds\z/, '').sub(/_kind\z/, '')
              candidate_cols = ["#{base}_type", "#{base}_status"]
              col = candidate_cols.find { |c| content.include?(c) }
              next unless col

              result[:models][model_name][:enums][col] ||= {}
              result[:models][model_name][:enums][col].merge!(mapping)
            end

            # scopes: scope :with_videos, -> { where(attachment_type: 0) }
            content.scan(/scope\s+:([a-zA-Z0-9_]+),\s*->\s*\{([^}]+)\}/m).each do |(name, body)|
              where = body.strip.gsub(/\s+/, ' ')
              result[:scopes][model_name][name] = { type: 'scope', arity: -1, where: where[0..200] }
            end
          end
        end
        result
      rescue StandardError => e
        @config.logger.warn("[code_to_query] Static scan failed: #{e.message}")
        { models: {}, associations: {}, validations: {}, scopes: {} }
      end

      def deep_merge_models(primary, extra)
        merged = Marshal.load(Marshal.dump(primary))
        %i[models associations validations scopes].each do |key|
          merged[key] ||= {}
          (extra[key] || {}).each do |k, v|
            merged[key][k] = if v.is_a?(Hash) && merged[key][k].is_a?(Hash)
                               merged[key][k].merge(v) { |_kk, a, b| a.is_a?(Hash) && b.is_a?(Hash) ? a.merge(b) : b }
                             else
                               v
                             end
          end
        end
        merged
      rescue StandardError
        primary
      end

      def infer_model_name_from_path(path)
        # app/models/question.rb -> Question
        base = File.basename(path, '.rb')
        return nil if base.empty?

        base.split('/').last.split('_').map(&:capitalize).join
      end

      def extract_join_hints(schema)
        tables = Array(schema[:tables] || schema['tables'] || [])
        fks = []
        tables.each do |t|
          tname = t[:name] || t['name']
          cols = Array(t[:columns] || t['columns'])
          cols.each do |c|
            cname = c[:name] || c['name']
            if cname.end_with?('_id')
              ref = cname.sub(/_id\z/, 's')
              fks << { from: tname, column: cname, to: ref }
            end
          end
        end
        fks
      rescue StandardError
        []
      end

      def list_tables(connection)
        adapter_name = connection.adapter_name.downcase
        @config.logger.info("[code_to_query] Detecting tables for adapter: #{adapter_name}")

        # Try Rails helpers first
        names = []
        begin
          if connection.respond_to?(:data_sources)
            names = Array(connection.data_sources)
            @config.logger.info("[code_to_query] Using data_sources method, found #{names.length} tables")
          else
            names = Array(connection.tables)
            @config.logger.info("[code_to_query] Using tables method, found #{names.length} tables")
          end
        rescue StandardError => e
          @config.logger.warn("[code_to_query] Rails helpers failed: #{e.message}")
          names = []
        end
        return names.uniq if names.any?

        # Fallback by adapter - use actual adapter name from connection
        case adapter_name
        when 'postgresql'
          @config.logger.info('[code_to_query] Trying PostgreSQL specific queries')
          # First try with search path
          begin
            search_path = connection.respond_to?(:schema_search_path) ? connection.schema_search_path.to_s : 'public'
            schemas = search_path.split(',').map { |s| s.strip.gsub('"', '') }
            @config.logger.info("[code_to_query] Using schemas: #{schemas.join(', ')}")
            sql = <<~SQL
              SELECT schemaname, tablename
              FROM pg_tables
              WHERE schemaname = ANY (ARRAY[#{schemas.map { |s| connection.quote(s) }.join(', ')}])
            SQL
            result = connection.execute(sql)
            pg_names = result.map { |r| r['tablename'] || r[:tablename] }.compact.uniq
            @config.logger.info("[code_to_query] Found #{pg_names.length} tables via pg_tables: #{pg_names.join(', ')}")
            return pg_names if pg_names.any?
          rescue StandardError => e
            @config.logger.warn("[code_to_query] pg_tables query failed: #{e.message}")
          end

          # Fallback to information_schema
          begin
            info = connection.execute(<<~SQL)
              SELECT table_schema, table_name
              FROM information_schema.tables
              WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('pg_catalog','information_schema')
            SQL
            info_names = info.map { |r| r['table_name'] || r[:table_name] }.compact.uniq
            @config.logger.info("[code_to_query] Found #{info_names.length} tables via information_schema: #{info_names.join(', ')}")
            return info_names if info_names.any?
          rescue StandardError => e
            @config.logger.warn("[code_to_query] information_schema query failed: #{e.message}")
          end
        when 'mysql2', 'mysql'
          @config.logger.info('[code_to_query] Trying MySQL specific queries')
          begin
            result = connection.execute("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
            mysql_names = result.map { |r| r.values.first }.compact.uniq
            @config.logger.info("[code_to_query] Found #{mysql_names.length} tables via SHOW TABLES: #{mysql_names.join(', ')}")
            return mysql_names if mysql_names.any?
          rescue StandardError => e
            @config.logger.warn("[code_to_query] SHOW TABLES query failed: #{e.message}")
          end
        when 'sqlite3', 'sqlite'
          @config.logger.info('[code_to_query] Trying SQLite specific queries')
          begin
            result = connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
            sqlite_names = result.map { |r| r['name'] || r[:name] }.compact.uniq
            @config.logger.info("[code_to_query] Found #{sqlite_names.length} tables via sqlite_master: #{sqlite_names.join(', ')}")
            return sqlite_names if sqlite_names.any?
          rescue StandardError => e
            @config.logger.warn("[code_to_query] sqlite_master query failed: #{e.message}")
          end
        else
          @config.logger.info("[code_to_query] Unknown adapter '#{adapter_name}', trying generic methods")
        end

        # Last resort: parse db/schema.rb if present
        @config.logger.info('[code_to_query] Trying to parse db/schema.rb as last resort')
        parsed = parse_schema_rb
        if parsed.any?
          @config.logger.info("[code_to_query] Found #{parsed.length} tables in schema.rb: #{parsed.join(', ')}")
          return parsed
        end

        @config.logger.info('[code_to_query] No tables found through any method')
        []
      end

      def parse_schema_rb
        return [] unless defined?(Rails)

        schema_path = Rails.root.join('db', 'schema.rb')
        unless File.exist?(schema_path)
          @config.logger.info("[code_to_query] schema.rb not found at #{schema_path}")
          return []
        end

        begin
          content = File.read(schema_path)
          # Match lines like: create_table "table_name", force: :cascade do |t|
          tables = content.scan(/create_table\s+"([^"]+)"/).flatten.uniq
          @config.logger.info("[code_to_query] Parsed #{tables.length} table names from schema.rb: #{tables.join(', ')}")
          tables
        rescue StandardError => e
          @config.logger.warn("[code_to_query] Failed to parse schema.rb: #{e.message}")
          []
        end
      end

      def extract_table_columns(connection, table_name)
        primary_key_name = connection.primary_key(table_name)

        connection.columns(table_name).map do |col|
          is_primary = col.name == primary_key_name

          {
            name: col.name,
            sql_type: col.sql_type,
            type: col.type,
            null: col.null,
            default: col.default,
            primary: is_primary,
            auto_increment: determine_auto_increment(col, connection: connection, is_primary: is_primary),
            comment: extract_column_comment(connection, table_name, col.name)
          }
        end
      rescue StandardError => e
        @config.logger.warn("[code_to_query] Failed to extract columns for #{table_name}: #{e.message}")
        []
      end

      def determine_auto_increment(column, connection: nil, is_primary: false)
        # Handle different database adapters and Rails versions
        return column.auto_increment? if column.respond_to?(:auto_increment?)
        return column.serial? if column.respond_to?(:serial?)
        return column.identity? if column.respond_to?(:identity?)

        # Fallback: check based on sql_type and database-specific patterns
        return false if column.sql_type.nil?

        sql_type_lower = column.sql_type.downcase

        case sql_type_lower
        when /serial/, /identity/
          # PostgreSQL serial, bigserial, identity columns
          true
        when /int.*auto_increment/, /auto_increment/
          # MySQL auto_increment columns
          true
        else
          # Check default value for sequence patterns (PostgreSQL)
          return true if column.default.to_s =~ /nextval\(/i

          # SQLite special case: INTEGER PRIMARY KEY is auto-increment
          if connection && @config.adapter == :sqlite && is_primary && (sql_type_lower == 'integer')
            return true
          end

          false
        end
      rescue StandardError
        false
      end

      def extract_table_indexes(connection, table_name)
        connection.indexes(table_name).map do |idx|
          {
            name: idx.name,
            columns: idx.columns,
            unique: idx.unique,
            partial: idx.try(:where).present?,
            type: idx.try(:type) || 'btree'
          }
        end
      rescue StandardError => e
        @config.logger.warn("[code_to_query] Failed to extract indexes for #{table_name}: #{e.message}")
        []
      end

      def extract_foreign_keys(connection, table_name)
        if connection.respond_to?(:foreign_keys)
          connection.foreign_keys(table_name).map do |fk|
            {
              name: fk.name,
              column: fk.column,
              to_table: fk.to_table,
              primary_key: fk.primary_key,
              on_delete: fk.on_delete,
              on_update: fk.on_update
            }
          end
        else
          []
        end
      rescue StandardError => e
        @config.logger.warn("[code_to_query] Failed to extract foreign keys for #{table_name}: #{e.message}")
        []
      end

      def extract_table_constraints(connection, table_name)
        constraints = []

        # Check constraints (PostgreSQL specific)
        if @config.adapter == :postgres
          begin
            check_constraints = connection.execute(<<~SQL)
              SELECT conname, pg_get_constraintdef(oid) as definition
              FROM pg_constraint#{' '}
              WHERE conrelid = '#{table_name}'::regclass#{' '}
              AND contype = 'c'
            SQL

            check_constraints.each do |row|
              constraints << {
                name: row['conname'],
                type: 'check',
                definition: row['definition']
              }
            end
          rescue StandardError
            # Ignore if we can't get check constraints
          end
        end

        constraints
      rescue StandardError => e
        @config.logger.warn("[code_to_query] Failed to extract constraints for #{table_name}: #{e.message}")
        []
      end

      def extract_column_comment(connection, table_name, column_name)
        case @config.adapter
        when :postgres
          result = connection.execute(<<~SQL)
            SELECT col_description(pgc.oid, pga.attnum) as comment
            FROM pg_class pgc
            JOIN pg_attribute pga ON pgc.oid = pga.attrelid
            WHERE pgc.relname = '#{table_name}'#{' '}
            AND pga.attname = '#{column_name}'
          SQL
          result.first&.fetch('comment', nil)
        when :mysql
          result = connection.execute(<<~SQL)
            SELECT COLUMN_COMMENT as comment
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = '#{table_name}'
            AND COLUMN_NAME = '#{column_name}'
          SQL
          result.first&.fetch('comment', nil)
        end
      rescue StandardError
        nil
      end

      def extract_schema_version(connection)
        case @config.adapter
        when :postgres
          connection.execute('SELECT version()').first['version']
        when :mysql
          connection.execute('SELECT version()').first['version()']
        when :sqlite
          connection.execute('SELECT sqlite_version()').first['sqlite_version()']
        else
          'unknown'
        end
      rescue StandardError
        'unknown'
      end

      def has_timestamps?(model)
        model.column_names.include?('created_at') && model.column_names.include?('updated_at')
      end

      def has_soft_delete?(model)
        model.column_names.include?('deleted_at') ||
          (model.respond_to?(:paranoid?) && model.paranoid?)
      end

      def extract_model_associations(model)
        associations = {}

        model.reflect_on_all_associations.each do |assoc|
          # Skip associations that point to non-existent classes
          begin
            # Check if the class exists by trying to constantize it
            assoc.class_name.constantize
          rescue NameError
            @config.logger.info("[code_to_query] Skipping association #{assoc.name} for #{model.name}: class #{assoc.class_name} not found")
            next
          rescue StandardError => e
            @config.logger.info("[code_to_query] Skipping association #{assoc.name} for #{model.name}: #{e.message}")
            next
          end

          # Additional check: verify the association doesn't cause errors
          begin
            associations[assoc.name] = {
              type: assoc.macro,
              class_name: assoc.class_name,
              foreign_key: assoc.foreign_key,
              primary_key: assoc.association_primary_key,
              through: assoc.options[:through],
              dependent: assoc.options[:dependent],
              polymorphic: assoc.options[:polymorphic],
              as: assoc.options[:as]
            }
          rescue StandardError => e
            @config.logger.info("[code_to_query] Skipping problematic association #{assoc.name} for #{model.name}: #{e.message}")
            next
          end
        end

        associations
      rescue StandardError => e
        @config.logger.warn("[code_to_query] Failed to extract associations for #{model.name}: #{e.message}")
        {}
      end

      def extract_model_validations(model)
        validations = {}

        model.validators.each do |validator|
          validator.attributes.each do |attr|
            validations[attr] ||= []
            validations[attr] << {
              type: validator.class.name,
              options: validator.options
            }
          end
        end

        validations
      rescue StandardError => e
        @config.logger.warn("[code_to_query] Failed to extract validations for #{model.name}: #{e.message}")
        {}
      end

      def extract_model_enums(model)
        enums = {}
        begin
          # 1) Native Rails enums
          if model.respond_to?(:defined_enums)
            model.defined_enums.each do |name, mapping|
              enums[name] = mapping
            end
          end

          # 2) Infer from mapping constants like ATTACHMENT_TYPES = { 'video'=>0, 'image'=>1 }
          begin
            column_names = model.column_names
          rescue StandardError
            column_names = []
          end

          model.constants(false).each do |const_name|
            value = model.const_get(const_name)
            next unless value.is_a?(Hash)
            # Ensure values are integers (or coercible) and keys are strings/symbols
            next unless value.keys.all? { |k| k.is_a?(String) || k.is_a?(Symbol) }
            next unless value.values.all? { |v| v.is_a?(Integer) || (v.is_a?(String) && v.match?(/^\d+$/)) }

            const_str = const_name.to_s
            # Heuristic: map *_TYPES, *_STATUS, *_STATUSES, *_KINDS to *_type/_status columns
            base = const_str.downcase
            base = base.sub(/_types\z/, '').sub(/_type\z/, '').sub(/_statuses\z/, '').sub(/_status\z/, '').sub(/_kinds\z/, '').sub(/_kind\z/, '')

            candidate_columns = []
            candidate_columns << (base.end_with?('_') ? "#{base}type" : "#{base}_type")
            candidate_columns << (base.end_with?('_') ? "#{base}status" : "#{base}_status")

            matched_column = candidate_columns.find { |c| column_names.include?(c) }
            next unless matched_column

            mapping = {}
            value.each do |k, v|
              label = k.to_s
              mapping[label] = Integer(v)
            end

            # Merge/augment if Rails enum already present for the same column
            enums[matched_column] = (enums[matched_column] || {}).merge(mapping)
          rescue StandardError
            next
          end

          # 3) Fallback via generated enum helper methods (e.g., attachment_types)
          begin
            column_names.each do |col|
              plural_method = if ''.respond_to?(:pluralize)
                                col.to_s.pluralize
                              else
                                "#{col}s"
                              end
              next unless model.respond_to?(plural_method)

              mapping = model.public_send(plural_method)
              next unless mapping.is_a?(Hash) && mapping.keys.all? { |k| k.is_a?(String) || k.is_a?(Symbol) }
              next unless mapping.values.all? { |v| v.is_a?(Integer) || (v.is_a?(String) && v.match?(/^\d+$/)) }

              normalized = {}
              mapping.each { |k, v| normalized[k.to_s] = Integer(v) }
              enums[col.to_s] = (enums[col.to_s] || {}).merge(normalized)
            end
          rescue StandardError
            # ignore
          end
        rescue StandardError => e
          @config.logger.warn("[code_to_query] Failed to extract enums for #{model.name}: #{e.message}")
        end
        enums
      end

      def extract_model_scopes(model)
        scopes = {}

        # Prefer explicit registries if available
        registry_names = []
        if model.respond_to?(:scope_registry) && model.scope_registry.respond_to?(:each)
          model.scope_registry.each do |name, body|
            registry_names << name.to_s
            scopes[name.to_s] = { type: 'scope', arity: (body.respond_to?(:arity) ? body.arity : 0) }
          end
        elsif model.respond_to?(:scopes)
          model.scopes.each do |name, scope_proc|
            registry_names << name.to_s
            scopes[name.to_s] = { type: 'scope', arity: scope_proc&.arity || 0 }
          end
        elsif model.respond_to?(:defined_scopes)
          model.defined_scopes.each_key do |name|
            registry_names << name.to_s
            scopes[name.to_s] = { type: 'scope', arity: 0 }
          end
        end

        # Fallback: probe singleton methods that look like scopes and return a Relation
        begin
          candidate_methods = model.singleton_methods(false).select { |m| m.to_s.match?(/\A[a-z_][a-zA-Z0-9_]*\z/) }
          disallow = %w[new create update delete destroy find where order limit select joins includes preload eager_load pluck first last all none not or count sum average minimum maximum]
          candidate_methods.each do |m|
            next if disallow.include?(m.to_s)
            next if registry_names.include?(m.to_s)

            meth = model.method(m)
            ar = meth.arity
            # Only try zero-arg or optional-arg methods
            next unless ar.zero? || ar.negative?

            rel = nil
            begin
              rel = meth.call
            rescue ArgumentError
              next
            rescue StandardError
              next
            end
            if defined?(ActiveRecord::Relation) && rel.is_a?(ActiveRecord::Relation)
              scopes[m.to_s] ||= { type: 'scope', arity: ar }
            end
          end
        rescue StandardError
          # ignore
        end

        # Enhance scope entries with sample SQL and a compact where summary
        scopes.each_key do |name|
          rel = model.public_send(name)
          if defined?(ActiveRecord::Relation) && rel.is_a?(ActiveRecord::Relation)
            sql = rel.limit(1).to_sql
            scopes[name][:sample_sql] = truncate(sql, 500)
            where = extract_where_clause(sql)
            scopes[name][:where] = truncate(where, 200) if where
          end
        rescue StandardError
          # skip scopes that error out
        end

        scopes
      rescue StandardError => e
        @config.logger.warn("[code_to_query] Failed to extract scopes for #{model.name}: #{e.message}")
        {}
      end

      def extract_where_clause(sql)
        return nil unless sql.is_a?(String)

        if (m = sql.match(/\bWHERE\s+(.+?)(?:\s+ORDER\s+BY|\s+LIMIT|\s+GROUP\s+BY|\z)/i))
          # Normalize quotes and whitespace
          m[1].gsub('"', '"').gsub(/\s+/, ' ').strip

        end
      end

      def truncate(str, max)
        return str unless str.is_a?(String)
        return str if str.length <= max

        "#{str[0, max - 3]}..."
      end

      def generate_table_synonyms(table_name)
        synonyms = []

        # Singular/plural variations
        synonyms << if table_name.end_with?('s')
                      table_name.chomp('s')
                    else
                      "#{table_name}s"
                    end

        # Basic common business synonyms - LLM will extend these based on actual schema
        business_synonyms = {}

        synonyms.concat(business_synonyms[table_name] || [])
        synonyms.uniq
      end

      def generate_column_synonyms(column_name, sql_type)
        synonyms = []

        # Basic common column synonyms - LLM will extend these based on actual schema
        column_synonyms = {
          'created_at' => %w[created_on date_created creation_date],
          'updated_at' => %w[updated_on date_updated modification_date],
          'email' => %w[email_address e_mail],
          'phone' => %w[phone_number telephone],
          'amount' => %w[total price cost value],
          'quantity' => %w[qty amount count],
          'description' => %w[desc summary details]
        }

        synonyms.concat(column_synonyms[column_name] || [])

        # Type-based synonyms
        if sql_type.to_s.match?(/money|decimal.*2|numeric.*2/) && !column_name.match?(/amount|price|cost/)
          synonyms.push(%w[amount price cost])
        end

        synonyms.uniq
      end

      def load_business_glossary
        glossary_path = File.join(File.dirname(@config.context_pack_path), 'business_glossary.json')

        if File.exist?(glossary_path)
          JSON.parse(File.read(glossary_path))
        else
          {}
        end
      rescue StandardError
        {}
      end

      def enrich_glossary_with_llm(base_glossary, schema_data, models_data)
        return base_glossary unless @config.auto_glossary_with_llm
        return base_glossary unless @config.openai_api_key

        # Build a compact prompt for the LLM to suggest synonyms and intent hints
        begin
          require 'net/http'
          require 'uri'
          require 'json'

          tables = Array(schema_data[:tables] || schema_data['tables'])
          table_summaries = tables.take(20).map do |t|
            name = t[:name] || t['name']
            cols = Array(t[:columns] || t['columns']).map { |c| c[:name] || c['name'] }
            fks = cols.select { |c| c.to_s.end_with?('_id') }
            "#{name}: cols(#{cols.take(12).join(', ')}), fks(#{fks.join(', ')})"
          end

          scopes = models_data[:scopes] || models_data['scopes'] || {}
          scope_lines = scopes.flat_map do |model_name, scope_hash|
            next [] unless scope_hash.is_a?(Hash)

            scope_hash.map do |scope_name, meta|
              where = meta[:where] || meta['where']
              "#{model_name}.#{scope_name}: #{where}" if where
            end.compact
          end.take(40)

          system_prompt = <<~P
            You are analyzing a database schema to understand the business domain and create a comprehensive glossary.

            Your task: Infer the business domain from table names, column names, and relationships, then create mappings that help users query with natural language.

            Analyze the schema to understand:
            1. What business domain this represents (e.g. e-commerce, education, CRM, content management, etc.)
            2. What real-world entities and relationships exist
            3. How a user would naturally refer to these entities and relationships

            Create mappings for:
            - Business entities: How users refer to main concepts
            - Relationships: How users describe connections between entities (e.g., "answered by", "created by", "belongs to")
            - Actions/States: How users describe actions or states (e.g., "completed", "failed", "pending")
            - IDs and Foreign Keys: How users refer to specific entities
            - Domain-specific terms: Technical terms users might use differently than column names

            For relationship queries, create special relationship mappings:
            - Key format: "relationship_[action]_[entity]"#{' '}
            - Value: Array describing the EXISTS pattern needed

            Rules:
            - Only use tables and columns that exist in the schema
            - Infer domain from naming patterns, foreign keys, and table relationships
            - Don't assume any specific business domain
            - Create practical mappings a real user would need
            - Output JSON: {"term": ["synonym1", "synonym2"], "table.column": ["user_term1"], "relationship_action_entity": ["EXISTS pattern hint"]}

            Keep under #{@config.max_glossary_suggestions} entries total.
          P

          # Analyze foreign key relationships to understand business logic
          fk_relationships = tables.flat_map do |t|
            table_name = t[:name] || t['name']
            fks = Array(t[:foreign_keys] || t['foreign_keys'])
            fks.map do |fk|
              "#{table_name}.#{fk[:column] || fk['column']} -> #{fk[:to_table] || fk['to_table']}"
            end
          end

          user_prompt = <<~U
            DATABASE SCHEMA ANALYSIS:

            Tables and Columns:
            #{table_summaries.join("\n")}

            Foreign Key Relationships (showing business connections):
            #{fk_relationships.join("\n")}

            Model Scopes (showing common business queries):
            #{scope_lines.join("\n")}

            Current glossary has: #{base_glossary.keys.take(20).join(', ')}

            TASK: Analyze this schema and infer the business domain. Create a glossary that maps how real users would naturally refer to these entities and relationships. Focus especially on understanding what the foreign key relationships tell us about the business logic.
          U

          messages = [
            { role: 'system', content: system_prompt },
            { role: 'user', content: user_prompt }
          ]

          client = @config.llm_client || CodeToQuery::LLMClient.new(@config)
          text = client.chat(messages: messages)
          suggestions = begin
            JSON.parse(text)
          rescue StandardError
            {}
          end
          if suggestions.is_a?(Hash)
            return base_glossary.merge(suggestions)
          end
        rescue StandardError => e
          @config.logger.warn("[code_to_query] LLM glossary enrichment failed: #{e.message}")
        end

        base_glossary
      end

      def extract_authorization_policies
        policies = {}

        # Check for Pundit policies
        policies[:pundit] = extract_pundit_policies if defined?(Pundit)

        # Check for CanCanCan abilities
        policies[:cancan] = extract_cancan_policies if defined?(CanCan)

        policies
      rescue StandardError => e
        warn "[code_to_query] Failed to extract authorization policies: #{e.message}"
        {}
      end

      def extract_pundit_policies
        # This would extract Pundit policy information
        # Implementation depends on your specific setup
        {}
      end

      def extract_cancan_policies
        # This would extract CanCanCan ability information
        # Implementation depends on your specific setup
        {}
      end

      def write_pack(pack)
        path = @config.context_pack_path
        dir = File.dirname(path)
        FileUtils.mkdir_p(dir)
        File.write(path, pack.to_json)

        # Also write a human-readable version
        readme_path = File.join(dir, 'README.md')
        File.write(readme_path, generate_context_readme(pack))
      end

      def generate_context_readme(pack)
        <<~README
          # CodeToQuery Context Pack

          Generated on: #{Time.now}
          Database Adapter: #{pack.schema[:adapter]}
          Schema Version: #{pack.schema[:version]}

          ## Tables (#{pack.schema[:tables].length})

          #{pack.schema[:tables].map do |table|
            "- **#{table[:name]}** (#{table[:columns].length} columns)"
          end.join("\n")}

          ## Models (#{pack.models[:models].length})

          #{pack.models[:models].map do |name, info|
            "- **#{name}** → `#{info[:table_name]}`"
          end.join("\n")}

          ## Glossary Terms (#{pack.glossary.length})

          #{pack.glossary.map do |term, synonyms|
            "- **#{term}**: #{Array(synonyms).join(', ')}"
          end.join("\n")}

          This context pack is used by CodeToQuery to understand your database schema,
          model relationships, and business terminology for accurate natural language
          query translation.
        README
      end
    end
    # rubocop:enable Metrics/ClassLength
  end
end
