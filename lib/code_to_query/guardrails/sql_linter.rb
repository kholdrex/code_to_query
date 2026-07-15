# frozen_string_literal: true

module CodeToQuery
  module Guardrails
    class SqlLinter
      DANGEROUS_FUNCTIONS = %w[
        load_file outfile dumpfile
        sys_exec sys_eval
        benchmark sleep pg_sleep
        version user database schema
        current_user current_database current_schema
        inet_server_addr inet_client_addr
      ].freeze

      # PostgreSQL's complete built-in server-side XML export family. These
      # functions read a relation, query, cursor, schema, or the whole current
      # database without putting the exported relations in a FROM/JOIN clause,
      # so every member can bypass this linter's table allowlist.
      POSTGRES_SERVER_SIDE_XML_EXPORT_FUNCTIONS = %w[
        table_to_xml table_to_xmlschema table_to_xml_and_xmlschema
        query_to_xml query_to_xmlschema query_to_xml_and_xmlschema
        cursor_to_xml cursor_to_xmlschema
        schema_to_xml schema_to_xmlschema schema_to_xml_and_xmlschema
        database_to_xml database_to_xmlschema database_to_xml_and_xmlschema
      ].freeze

      POSTGRES_QUOTED_FUNCTION_IDENTIFIER = /
        (?<!")"(?<identifier>(?:""|[^"])*)"\s*\(
      /x

      POSTGRES_UNICODE_FUNCTION_IDENTIFIER = /
        (?<![A-Z0-9_$"])U&"(?<identifier>(?:""|[^"])*)"
      /ix

      def initialize(config, allow_tables: nil)
        @config = config
        @allow_tables = Array(allow_tables).compact.map(&:to_s)
      end

      def check!(sql)
        normalized = sql.to_s.strip.gsub(/\s+/, ' ')

        check_statement_type!(normalized)
        check_unsupported_table_query_expressions!(normalized)
        check_dangerous_patterns!(normalized)
        check_required_limit!(normalized)
        check_table_allowlist!(normalized) if @allow_tables.any?
        check_no_postgres_server_side_xml_export_functions!(normalized)
        check_no_literals!(normalized)
        check_no_dangerous_functions!(normalized)
        check_no_subqueries!(normalized) if @config.block_subqueries
        check_join_complexity!(normalized)

        true
      end

      private

      def check_statement_type!(sql)
        raise SecurityError, 'Only SELECT statements are allowed' unless sql =~ /\A\s*SELECT\b/i

        raise SecurityError, 'Multiple statements and semicolons are not allowed' if sql.count(';').positive?

        dangerous_keywords = %w[
          DROP ALTER CREATE INSERT UPDATE DELETE TRUNCATE
          GRANT REVOKE EXEC EXECUTE CALL
        ]

        dangerous_keywords.each do |keyword|
          raise SecurityError, "Dangerous keyword '#{keyword}' is not allowed" if sql.match?(/\b#{keyword}\b/i)
        end
      end

      def check_unsupported_table_query_expressions!(sql)
        return unless %i[postgres postgresql].include?(@config.adapter.to_sym)
        return unless Query::SqlScanner.new.table_query_expression?(sql)

        raise SecurityError, 'PostgreSQL TABLE query expressions are not supported'
      end

      def check_dangerous_patterns!(sql)
        patterns = build_dangerous_patterns
        patterns.each do |pattern|
          raise SecurityError, "Dangerous SQL pattern detected: #{pattern.inspect}" if sql.match?(pattern)
        end

        check_encoding_bypass!(sql)
        check_polyglot_attacks!(sql)
        check_time_delay_patterns!(sql)
      end

      def build_dangerous_patterns
        generic = [
          /;\s*--/i,
          %r{;\s*/\*}i,
          /'\s*;\s*/i,
          /\bunion\b.*?\bselect\b/i,
          /\bor\s+1\s*=\s*1\b/i,
          /0x[0-9a-f]+/i, # hex literal (often MySQL)
          /'\s*\+\s*'/,
          /'\s*or\s+'/i,
          /'\s*and\s+'/i,
          /'\s*union\s+'/i,
          /'\s*;\s*'/,
          /\\'/,
          /%27/,
          /%3B/,
          %r{/\*.*?\*/}m,
          /--[^\r\n]*/,
          /\#[^\r\n]*/,
          /\bcast\s*\(.*?\bas\b/i,
          /;\s*(drop|create|alter|insert|update|delete)\s+/i,
          /information_schema/i,
          /into\s+outfile/i,
          /into\s+dumpfile/i
        ]

        case @config.adapter
        when :postgres, :postgresql
          generic + [
            /\|\|/, # concat
            /pg_\w+/i, # pg functions
            /pg_sleep\s*\(/i # time-based
          ]
        when :mysql
          generic + [
            /%20(union|or|and)%20/i,
            /unhex\s*\(/i,
            /benchmark\s*\(/i,
            /sleep\s*\(/i,
            /extractvalue\s*\(/i,
            /updatexml\s*\(/i,
            /into\s+outfile/i,
            /into\s+dumpfile/i,
            /mysql\./i,
            /performance_schema/i
          ]
        when :sqlserver, :mssql
          generic + [
            /\+\s*SELECT/i,
            /waitfor\s+delay/i,
            /\bxp_\w+\b/i,
            /\bsp_\w+\b/i,
            /\bfn_\w+\b/i,
            /msdb\./i,
            /tempdb\./i
          ]
        when :sqlite
          generic + [
            /sqlite_master/i
          ]
        else
          generic
        end
      end

      def check_required_limit!(sql)
        # COUNT queries: no LIMIT required
        return if is_count_query?(sql)

        # Aggregations (excluding COUNT): if configured, require and enforce per-type limit
        if is_non_count_aggregation_query?(sql)
          unless allows_unlimited_aggregations?
            raise SecurityError, 'LIMIT clause is required for aggregation queries' unless sql =~ /\bLIMIT\s+\d+\b/i

            enforce_per_type_limit!(sql, @config.aggregation_limit) if @config.aggregation_limit
          end
          enforce_global_max_limit!(sql)
          return
        end

        # DISTINCT queries: if configured, require and enforce per-type limit
        if has_distinct?(sql)
          unless allows_unlimited_distinct?
            raise SecurityError, 'LIMIT clause is required for DISTINCT queries' unless sql =~ /\bLIMIT\s+\d+\b/i

            enforce_per_type_limit!(sql, @config.distinct_limit) if @config.distinct_limit
          end
          enforce_global_max_limit!(sql)
          return
        end

        # EXISTS: usually controlled by compiler via LIMIT 1; don't enforce here
        return if sql.match?(/\bEXISTS\s*\(/i)

        # Default: require limit for SELECT queries
        if requires_limit?(sql) && sql !~ /\bLIMIT\s+\d+\b/i
          raise SecurityError, 'LIMIT clause is required for this query type'
        end

        enforce_global_max_limit!(sql)
      end

      def requires_limit?(_sql)
        # Respect configuration: require LIMIT only if enabled
        !!@config.require_limit_by_default
      end

      def enforce_global_max_limit!(sql)
        limit_match = sql.match(/\bLIMIT\s+(\d+)\b/i)
        return unless limit_match

        limit_value = limit_match[1].to_i
        max_limit = @config.max_limit || 10_000

        raise SecurityError, "LIMIT value #{limit_value} exceeds maximum allowed (#{max_limit})" if limit_value > max_limit
        raise SecurityError, 'LIMIT value must be positive' if limit_value <= 0
      end

      def enforce_per_type_limit!(sql, per_type_limit)
        return unless per_type_limit

        limit_match = sql.match(/\bLIMIT\s+(\d+)\b/i)
        return unless limit_match

        limit_value = limit_match[1].to_i
        raise SecurityError, "LIMIT value #{limit_value} exceeds per-type maximum (#{per_type_limit})" if limit_value > per_type_limit
      end

      def has_distinct?(sql)
        sql.match?(/\bDISTINCT\b/i)
      end

      def is_count_query?(sql)
        sql.match?(/\bCOUNT\s*\(/i)
      end

      def is_non_count_aggregation_query?(sql)
        sql.match?(/\b(SUM|AVG|MAX|MIN)\s*\(/i)
      end

      def has_count_or_aggregation?(sql)
        sql.match?(/\b(COUNT|SUM|AVG|MAX|MIN)\s*\(/i)
      end

      def allows_unlimited_aggregations?
        @config.aggregation_limit.nil? && @config.count_limit.nil?
      end

      def allows_unlimited_distinct?
        @config.distinct_limit.nil?
      end

      def check_table_allowlist!(sql)
        # Extract table names from FROM and JOIN clauses
        referenced_tables = extract_table_names(sql)

        referenced_tables.each do |table|
          unless table_allowed?(table)
            raise SecurityError, "Table '#{table}' is not in the allowed list: #{@allow_tables.join(', ')}"
          end
        end
      end

      def table_allowed?(table)
        # MySQL table-name case semantics vary by lower_case_table_names and
        # host filesystem. Fail closed with exact matching when they are not
        # explicitly available to this legacy linter.
        return @allow_tables.include?(table.to_s) if @config.adapter.to_sym == :mysql

        @allow_tables.any? { |allowed| IdentifierSemantics.ascii_case_insensitive?(allowed, table) }
      end

      def check_no_literals!(sql)
        # Block string literals (except in very specific contexts)
        # Allow some specific cases like LIKE patterns that might be pre-sanitized
        if sql.match?(/'[^']*'/) && !(sql.match?(/\bLIKE\s+\$\d+\b/i) || sql.match?(/\bILIKE\s+\$\d+\b/i))
          raise SecurityError, 'String literals are not allowed; use parameterized queries'
        end

        # Block numeric literals in WHERE clauses (except for common safe values)
        where_match = sql.match(/\bWHERE\s+(.+?)(?:\s+ORDER\s+BY|\s+LIMIT|\s+GROUP\s+BY|\Z)/i)
        return unless where_match

        where_clause = where_match[1]
        # Allow 0, 1, -1, and placeholder patterns
        return unless where_clause.match?(/\b\d{2,}\b/) && !where_clause.match?(/\$\d+|\?/)

        raise SecurityError, 'Numeric literals in WHERE clauses should be parameterized'
      end

      def check_no_dangerous_functions!(sql)
        DANGEROUS_FUNCTIONS.each do |func|
          raise SecurityError, "Dangerous function '#{func}' is not allowed" if sql.match?(/\b#{func}\s*\(/i)
        end
      end

      def check_no_postgres_server_side_xml_export_functions!(sql)
        return unless %i[postgres postgresql].include?(@config.adapter.to_sym)

        scanner = Query::SqlScanner.new
        quoted_searchable = scanner.mask_literals_and_comments(sql)
        executable_searchable = scanner.mask_literals_comments_and_identifier_contents(sql)

        POSTGRES_SERVER_SIDE_XML_EXPORT_FUNCTIONS.each do |func|
          next unless postgres_unquoted_function_call?(executable_searchable, func)

          raise SecurityError, "Dangerous function '#{func}' is not allowed"
        end

        quoted_searchable.scan(POSTGRES_QUOTED_FUNCTION_IDENTIFIER) do
          identifier = Regexp.last_match[:identifier].gsub('""', '"')
          next unless POSTGRES_SERVER_SIDE_XML_EXPORT_FUNCTIONS.include?(identifier)

          raise SecurityError, "Dangerous function '#{identifier}' is not allowed"
        end

        quoted_searchable.scan(POSTGRES_UNICODE_FUNCTION_IDENTIFIER) do
          match = Regexp.last_match
          escape, function_call = postgres_unicode_function_escape(sql, match.end(0))
          next unless function_call

          unless escape
            raise SecurityError, 'Inconclusive PostgreSQL Unicode function identifier'
          end

          identifier = decode_postgres_unicode_identifier(match[:identifier], escape)
          unless identifier
            raise SecurityError, 'Inconclusive PostgreSQL Unicode function identifier'
          end
          next unless POSTGRES_SERVER_SIDE_XML_EXPORT_FUNCTIONS.include?(identifier)

          raise SecurityError, "Dangerous function '#{identifier}' is not allowed"
        end
      end

      def postgres_unicode_function_escape(sql, index)
        whitespace_start = index
        index += 1 while sql[index]&.match?(/\s/)
        escape = '\\'

        if index > whitespace_start && sql[index, 7]&.casecmp?('UESCAPE') &&
           !postgres_identifier_continuation?(sql[index + 7])
          index += 7
          index += 1 while sql[index]&.match?(/\s/)
          escape, index = postgres_uescape_literal(sql, index)
          return [nil, true] unless index

          index += 1 while sql[index]&.match?(/\s/)
        end

        [escape, sql[index] == '(']
      end

      def postgres_uescape_literal(sql, index)
        escape_string = sql[index]&.casecmp?('E') && sql[index + 1] == "'"
        index += 1 if escape_string
        return [nil, nil] unless sql[index] == "'"

        index += 1
        value = +''
        while index < sql.length
          if sql[index] == "'" && sql[index + 1] == "'"
            value << "'"
            index += 2
          elsif sql[index] == "'"
            return [value.length == 1 ? value : nil, index + 1]
          elsif escape_string && sql[index] == '\\'
            character, index = postgres_escape_string_character(sql, index + 1)
            return [nil, nil] unless index

            value << character
          else
            value << sql[index]
            index += 1
          end
        end
        [nil, nil]
      end

      def postgres_escape_string_character(sql, index)
        return [nil, nil] unless sql[index]

        simple = { 'b' => "\b", 'f' => "\f", 'n' => "\n", 'r' => "\r", 't' => "\t" }
        return [simple.fetch(sql[index], sql[index]), index + 1] unless sql[index].match?(/[0-7xXuU]/)

        pattern, base = case sql[index]
                        when /[0-7]/ then [/[0-7]{1,3}/, 8]
                        when /[xX]/ then [/[0-9A-F]{1,2}/i, 16]
                        when 'u' then [/[0-9A-F]{4}/i, 16]
                        when 'U' then [/[0-9A-F]{8}/i, 16]
                        end
        digit_index = sql[index].match?(/[xXuU]/) ? index + 1 : index
        digits = sql[digit_index..]&.match(/\A#{pattern}/)&.[](0)
        return [nil, nil] unless digits

        [digits.to_i(base).chr(Encoding::UTF_8), digit_index + digits.length]
      rescue RangeError
        [nil, nil]
      end

      def postgres_unquoted_function_call?(sql, function)
        sql.to_enum(:scan, /#{Regexp.escape(function)}\s*\(/i).any? do
          match = Regexp.last_match
          previous = match.begin(0).positive? ? sql[match.begin(0) - 1] : nil
          following = sql[match.begin(0) + function.length]

          !postgres_identifier_continuation?(previous) && !postgres_identifier_continuation?(following)
        end
      end

      def postgres_identifier_continuation?(character)
        character && (!character.ascii_only? || character.match?(/[A-Z0-9_$]/i))
      end

      def decode_postgres_unicode_identifier(identifier, escape)
        return if escape.match?(/[0-9A-F+'"\s]/i)

        characters = identifier.gsub('""', '"').chars
        decoded = +''
        index = 0

        while index < characters.length
          if characters[index] != escape
            decoded << characters[index]
            index += 1
            next
          end

          codepoint, consumed = postgres_unicode_escape(characters, index, escape)
          return unless codepoint

          if codepoint.between?(0xD800, 0xDBFF)
            low_surrogate, low_consumed = postgres_unicode_escape(characters, index + consumed, escape)
            return unless low_surrogate&.between?(0xDC00, 0xDFFF)

            codepoint = 0x10000 + ((codepoint - 0xD800) << 10) + low_surrogate - 0xDC00
            consumed += low_consumed
          end

          decoded << codepoint.chr(Encoding::UTF_8)
          index += consumed
        end

        decoded
      rescue RangeError
        nil
      end

      def postgres_unicode_escape(characters, index, escape)
        return unless characters[index] == escape
        return [escape.ord, 2] if characters[index + 1] == escape

        extended = characters[index + 1] == '+'
        digits = extended ? 6 : 4
        start = index + (extended ? 2 : 1)
        hexadecimal = characters[start, digits]&.join
        return unless hexadecimal&.match?(/\A[0-9A-F]{#{digits}}\z/i)

        [hexadecimal.to_i(16), digits + (extended ? 2 : 1)]
      end

      def check_no_subqueries!(sql)
        return unless sql.match?(/\(\s*SELECT\b/i)

        raise SecurityError, 'Subqueries are not allowed in this context'
      end

      def check_join_complexity!(sql)
        # Count JOIN operations
        join_count = sql.scan(/\bJOIN\b/i).length
        max_joins = @config.max_joins || 3

        raise SecurityError, "Too many JOINs (#{join_count}); maximum allowed: #{max_joins}" if join_count > max_joins

        # Ensure JOINs have explicit conditions
        return unless join_count.positive? && !sql.match?(/\bON\b/i)

        raise SecurityError, 'JOINs must have explicit ON conditions'
      end

      def check_encoding_bypass!(sql)
        # Check for various encoding bypass attempts
        encoding_patterns = [
          /%2[0-9a-f]/i,          # URL encoding
          /&#x?[0-9a-f]+;/i,      # HTML entity encoding
          /\\x[0-9a-f]{2}/i,      # Hex escape sequences
          /\\[0-7]{3}/,           # Octal escape sequences
          /\\u[0-9a-f]{4}/i # Unicode escape sequences
        ]

        encoding_patterns.each do |pattern|
          raise SecurityError, "Potential encoding bypass detected: #{pattern.inspect}" if sql.match?(pattern)
        end
      end

      def check_polyglot_attacks!(sql)
        # Check for polyglot SQL attacks that work across multiple databases
        polyglot_patterns = [
          /select.*from.*information_schema.*union.*select/i,
          /\bor\b.*\bsleep\b.*\band\b.*\bbenchmark\b/i,
          /union.*select.*version\(\).*database\(\)/i,
          /\bif\s*\(\s*1\s*=\s*1\s*,\s*sleep\s*\(/i
        ]

        polyglot_patterns.each do |pattern|
          raise SecurityError, "Potential polyglot attack detected: #{pattern.inspect}" if sql.match?(pattern)
        end
      end

      def check_time_delay_patterns!(sql)
        # Advanced time-based attack detection
        time_patterns = [
          /waitfor\s+delay\s+['"]\d+:\d+:\d+['"]/i,  # SQL Server specific delay
          /select\s+sleep\s*\(\s*\d+\s*\)/i,         # MySQL sleep
          /select\s+pg_sleep\s*\(\s*\d+\s*\)/i,      # PostgreSQL sleep
          /benchmark\s*\(\s*\d+\s*,\s*.+?\)/i,       # MySQL benchmark
          /\bif\s*\(.+?,\s*sleep\s*\(/i # Conditional time delays
        ]

        time_patterns.each do |pattern|
          raise SecurityError, "Time-based attack pattern detected: #{pattern.inspect}" if sql.match?(pattern)
        end
      end

      def extract_table_names(sql)
        tables = []

        # Extract FROM clause tables (improved regex)
        from_matches = sql.scan(/\bFROM\s+(?:`([^`]+)`|"([^"]+)"|'([^']+)'|([a-zA-Z0-9_]+)(?:\s+(?:AS\s+)?[a-zA-Z_][a-zA-Z0-9_]*)?)/i)
        from_matches.each do |match|
          table_name = match.compact.first
          tables << table_name if table_name
        end

        # Extract JOIN clause tables (improved regex)
        join_matches = sql.scan(/\b(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\s+(?:`([^`]+)`|"([^"]+)"|'([^']+)'|([a-zA-Z0-9_]+)(?:\s+(?:AS\s+)?[a-zA-Z_][a-zA-Z0-9_]*)?)/i)
        join_matches.each do |match|
          table_name = match.compact.first
          tables << table_name if table_name
        end

        tables.uniq
      end
    end
  end
end
