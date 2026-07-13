# frozen_string_literal: true

module CodeToQuery
  class Query
    # Internal SQL scanning helpers used to enforce table allowlists while
    # preserving quoted literals, identifiers, and comment boundaries.
    class SqlScanner
      TableIdentifier = Struct.new(:name, :quoted, keyword_init: true)

      # PostgreSQL keywords that cannot be used as an unquoted relation name.
      # Keeping this distinction in the scanner matters because words such as
      # IS after `(table` are expression grammar, not the relation operand of a
      # TABLE query expression. Quoted words remain valid relation names.
      POSTGRES_RESERVED_KEYWORDS = %w[
        ALL ANALYSE ANALYZE AND ANY ARRAY AS ASC ASYMMETRIC BOTH CASE CAST CHECK
        COLLATE COLUMN CONSTRAINT CREATE CURRENT_CATALOG CURRENT_DATE CURRENT_ROLE
        CURRENT_TIME CURRENT_TIMESTAMP CURRENT_USER DEFAULT DEFERRABLE DESC DISTINCT
        DO ELSE END EXCEPT FALSE FETCH FOR FOREIGN FREEZE FROM FULL GRANT GROUP
        HAVING ILIKE IN INITIALLY INNER INTERSECT INTO IS ISNULL JOIN LATERAL LEADING
        LEFT LIKE LIMIT LOCALTIME LOCALTIMESTAMP NATURAL NOT NOTNULL NULL NULLS OFFSET
        ON ONLY OR ORDER OUTER OVERLAPS PLACING PRIMARY REFERENCES RETURNING RIGHT
        SELECT SESSION_USER SIMILAR SOME SYMMETRIC TABLE THEN TO TRAILING TRUE UNION
        UNIQUE USER USING VARIADIC VERBOSE WHEN WHERE WINDOW WITH
      ].freeze

      def strip_exists_subqueries(sql)
        source = sql.to_s
        searchable = mask_sql_literals_comments_and_identifier_contents(source)
        stripped = +''
        index = 0

        while index < source.length
          match = /\b(?:NOT\s+)?EXISTS\s*\(/i.match(searchable, index)
          break unless match

          stripped << source[index...match.begin(0)]
          stripped << 'TRUE'
          index = skip_parenthesized_sql(source, match.end(0))
        end

        stripped << source[index..] if index < source.length
        stripped
      end

      def exists_subqueries(sql, source_offset: 0)
        source = sql.to_s
        searchable = mask_sql_literals_comments_and_identifier_contents(source)
        subqueries = []
        index = 0

        while (match = /\b(NOT\s+)?EXISTS\s*\(/i.match(searchable, index))
          boundary = skip_parenthesized_sql(source, match.end(0))
          body_start = match.end(0)
          body = source[body_start...(boundary - 1)]
          subqueries << {
            operator: match[1] ? 'not_exists' : 'exists', sql: body,
            start: source_offset + body_start, finish: source_offset + boundary - 1
          }
          subqueries.concat(exists_subqueries(body, source_offset: source_offset + body_start))
          index = boundary
        end

        subqueries
      end

      def bind_placeholder_positions(sql)
        searchable = mask_sql_literals_comments_and_identifier_contents(sql.to_s)
        if searchable.match?(/\$\d+/)
          searchable.to_enum(:scan, /\$(\d+)/).map do
            [Regexp.last_match.begin(0), Regexp.last_match(1).to_i]
          end
        else
          ordinal = 0
          searchable.to_enum(:scan, /\?/).map do
            ordinal += 1
            [Regexp.last_match.begin(0), ordinal]
          end
        end
      end

      # PostgreSQL's TABLE relation query is a SELECT-equivalent and can occur
      # wherever a query operand is accepted. Tokenize code (with literals,
      # comments, and quoted identifier contents masked) so PostgreSQL's broad
      # unquoted identifier syntax is handled without mistaking TABLE inside
      # data or identifiers.
      def table_query_expression?(sql)
        tokens = sql_tokens(sql.to_s)
        tokens.each_with_index.any? do |token, index|
          next false unless keyword_token?(token, 'TABLE')
          next false unless query_operand_boundary?(tokens, index)

          relation_finish = table_relation_operand_finish(tokens, index + 1)
          relation_finish && table_query_continuation?(tokens[relation_finish])
        end
      end

      def policy_predicate_bind?(sql, table, column, bind_number, adapter:)
        policy_predicate_bind_numbers(
          sql, table, column, adapter: adapter, question_bind_number: bind_number, source_sql: sql
        ).include?(bind_number)
      end

      # Identify binds used as values of the expected qualified policy column.
      # A placeholder elsewhere in the query does not enforce policy.
      def policy_predicate_bind_numbers(sql, table, column, adapter: :postgres, question_bind_number: nil,
                                        source_sql: sql, source_offset: 0)
        return [] if table.to_s.empty? || column.to_s.empty?

        searchable = mask_sql_literals_and_comments(sql.to_s)
        where_body, where_offset = top_level_where_body(searchable)
        return [] unless where_body

        conjuncts = top_level_conjuncts(where_body)
        return [] unless conjuncts

        qualified = "#{policy_identifier_pattern(table, adapter, table: true)}\\s*\\.\\s*" \
                    "#{policy_identifier_pattern(column, adapter, table: false)}"
        wrappers = ->(predicate) { /\A\s*\(*\s*#{predicate}\s*\)*\s*\z/ }
        question_pattern = wrappers.call(
          "#{qualified}\\s*(?:=\\s*\\?|(?i:BETWEEN)\\s*\\?\\s+(?i:AND)\\s*\\?)"
        )
        placeholder_positions = bind_placeholder_positions(source_sql)
        first_placeholder = placeholder_positions.first
        question_placeholders = first_placeholder && source_sql.to_s[first_placeholder.first] == '?'
        if question_bind_number && question_placeholders
          placeholder_ordinals = placeholder_positions.to_h
          search_from = 0
          conjuncts.each do |conjunct|
            conjunct_offset = where_body.index(conjunct, search_from)
            search_from = conjunct_offset + conjunct.length
            next unless (match = question_pattern.match(conjunct))

            match[0].to_enum(:scan, /\?/).each do
              predicate_offset = where_offset + conjunct_offset + match.begin(0)
              local_position = predicate_offset + Regexp.last_match.begin(0)
              ordinal = placeholder_ordinals[source_offset + local_position]
              return [ordinal] if ordinal == question_bind_number
            end
          end
          return []
        end

        pattern = wrappers.call(
          "#{qualified}\\s*(?:=\\s*\\$(\\d+)|(?i:BETWEEN)\\s*\\$(\\d+)\\s+(?i:AND)\\s*\\$(\\d+))"
        )
        conjuncts.flat_map do |conjunct|
          match = pattern.match(conjunct)
          match ? match.captures.compact.map(&:to_i) : []
        end.uniq
      end

      private

      def sql_tokens(source)
        searchable = mask_sql_literals_comments_and_identifier_contents(source)
        tokens = []
        index = 0
        while index < searchable.length
          if searchable[index].match?(/\s/)
            index += 1
          elsif postgres_unicode_quoted_identifier_start?(searchable, index)
            finish = quoted_identifier_finish(source, index + 2)
            tokens << {
              type: :identifier, value: source[index...finish], quoted: true, unicode_quoted: true
            }
            index = finish
          elsif searchable[index] == '"'
            finish = quoted_identifier_finish(source, index)
            tokens << { type: :identifier, value: source[index...finish], quoted: true }
            index = finish
          elsif postgres_identifier_start?(searchable[index])
            finish = index + 1
            finish += 1 while postgres_identifier_continuation?(searchable[finish])
            tokens << { type: :identifier, value: searchable[index...finish] }
            index = finish
          else
            tokens << { type: :symbol, value: searchable[index] }
            index += 1
          end
        end
        tokens
      end

      def quoted_identifier_finish(source, index)
        index += 1
        while index < source.length
          if source[index] == '"' && source[index + 1] == '"'
            index += 2
          elsif source[index] == '"'
            return index + 1
          else
            index += 1
          end
        end
        source.length
      end

      # PostgreSQL lexes U&"..." (with no whitespace around the ampersand) as
      # one Unicode-escaped delimited identifier. Treating U, &, and the quoted
      # portion as separate tokens would miss a valid TABLE relation operand.
      def postgres_unicode_quoted_identifier_start?(searchable, index)
        searchable[index]&.casecmp?('U') && searchable[index + 1] == '&' && searchable[index + 2] == '"'
      end

      # PostgreSQL's lexer permits any non-ASCII character in an unquoted
      # identifier, including characters Unicode classifies as symbols. Avoid
      # a Unicode letter/category allowlist here: it would miss executable
      # relation names such as an emoji.
      def postgres_identifier_start?(character)
        return false unless character

        !character.ascii_only? || character == '_' || character.match?(/[A-Za-z]/)
      end

      def postgres_identifier_continuation?(character)
        postgres_identifier_start?(character) || character&.match?(/[0-9$]/)
      end

      def keyword_token?(token, keyword)
        token && token[:type] == :identifier && token[:value].casecmp?(keyword)
      end

      def postgres_relation_identifier_token?(token)
        return false unless token&.fetch(:type, nil) == :identifier
        return true if token[:quoted]

        POSTGRES_RESERVED_KEYWORDS.none? { |keyword| token[:value].casecmp?(keyword) }
      end

      def postgres_relation_identifier_finish(tokens, index)
        token = tokens[index]
        return unless postgres_relation_identifier_token?(token)

        index += 1
        # PostgreSQL permits an optional UESCAPE string after each U& quoted
        # identifier. String literal contents are deliberately absent from the
        # token stream, so consuming UESCAPE here reaches the next SQL token.
        index += 1 if token[:unicode_quoted] && keyword_token?(tokens[index], 'UESCAPE')
        index
      end

      # Parse TABLE's complete relation operand before deciding whether TABLE
      # starts a query expression. At an operand boundary, `(table IS NULL)`
      # and `(TABLE accounts)` initially look alike; the token after the
      # relation is what distinguishes expression grammar from query grammar.
      def table_relation_operand_finish(tokens, index)
        index += 1 if keyword_token?(tokens[index], 'ONLY')
        parenthesized = tokens[index]&.fetch(:value, nil) == '('
        index += 1 if parenthesized
        index = postgres_relation_identifier_finish(tokens, index)
        return unless index

        while tokens[index]&.fetch(:value, nil) == '.'
          index = postgres_relation_identifier_finish(tokens, index + 1)
          return unless index
        end
        if parenthesized
          return unless tokens[index]&.fetch(:value, nil) == ')'

          index += 1
        end
        index += 1 if tokens[index]&.fetch(:value, nil) == '*'
        index
      end

      def table_query_continuation?(token)
        return true unless token
        return true if token[:value] == ')'

        %w[UNION INTERSECT EXCEPT ORDER LIMIT OFFSET FETCH FOR].any? do |keyword|
          keyword_token?(token, keyword)
        end
      end

      def query_operand_boundary?(tokens, index)
        return true if index.zero?

        previous = tokens[index - 1]
        return true if previous&.fetch(:value, nil) == '('
        return true if %w[UNION INTERSECT EXCEPT].any? { |keyword| keyword_token?(previous, keyword) }
        return true if cte_query_operand_boundary?(tokens, index)
        return false unless %w[ALL DISTINCT].any? { |keyword| keyword_token?(previous, keyword) }

        %w[UNION INTERSECT EXCEPT].any? { |keyword| keyword_token?(tokens[index - 2], keyword) }
      end

      # At the outer level of a WITH operand, CTE definitions end in a closing
      # parenthesis immediately before the main SELECT/TABLE/VALUES expression.
      # Walk back over those balanced definitions to distinguish that position
      # from an ordinary identifier or alias named `table`.
      def cte_query_operand_boundary?(tokens, index)
        depth = 0
        cursor = index - 1
        while cursor >= 0
          token = tokens[cursor]
          value = token[:value]
          if value == ')'
            depth += 1
          elsif value == '('
            return false if depth.zero?

            depth -= 1
          elsif depth.zero?
            return query_operand_boundary?(tokens, cursor) if keyword_token?(token, 'WITH')
            return false if %w[SELECT VALUES TABLE].any? { |keyword| keyword_token?(token, keyword) }
            return false if value == ';'
          end
          cursor -= 1
        end
        false
      end

      def top_level_where_body(searchable)
        structure = mask_sql_literals_comments_and_identifier_contents(searchable)
        depth = 0
        where_end = nil
        structure.to_enum(:scan, /\bWHERE\b|[()]/i).each do
          token = Regexp.last_match(0)
          if token == '('
            depth += 1
          elsif token == ')'
            return [nil, nil] if depth.zero?

            depth -= 1
          elsif depth.zero?
            where_end = Regexp.last_match.end(0)
            break
          end
        end
        return [nil, nil] unless where_end

        finish = searchable.length
        suffix = structure[where_end..]
        suffix.to_enum(:scan, /\b(?:GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|OFFSET|FETCH|FOR|UNION|INTERSECT|EXCEPT|RETURNING)\b|[()]/i).each do
          token = Regexp.last_match(0)
          if token == '('
            depth += 1
          elsif token == ')'
            return [nil, nil] if depth.zero?

            depth -= 1
          elsif depth.zero?
            finish = where_end + Regexp.last_match.begin(0)
            break
          end
        end
        return [nil, nil] unless depth.zero?

        [searchable[where_end...finish], where_end]
      end

      def policy_identifier_pattern(value, adapter, table:)
        escaped = Regexp.escape(value.to_s)
        ascii_folded = IdentifierSemantics.ascii_case_insensitive_pattern(value)
        boundary = '[\\p{L}\\p{M}\\p{N}\\p{Pc}$]'
        exact_unquoted = "(?<!#{boundary})#{escaped}(?!#{boundary})"
        folded_unquoted = "(?<!#{boundary})#{ascii_folded}(?!#{boundary})"

        case adapter.to_sym
        when :postgres, :postgresql
          alternatives = ["\"#{escaped}\""]
          alternatives << folded_unquoted if value.to_s == IdentifierSemantics.ascii_fold(value)
        when :mysql
          identifier = table ? escaped : ascii_folded
          alternatives = ["`#{identifier}`", table ? exact_unquoted : folded_unquoted]
        else # SQLite resolves quoted and unquoted identifiers case-insensitively.
          alternatives = ["\"#{ascii_folded}\"", "`#{ascii_folded}`", folded_unquoted]
        end
        "(?:#{alternatives.join('|')})"
      end

      # Return mandatory top-level WHERE conjuncts. Nested expressions stay
      # intact, so policy text under NOT, CASE, or a subquery cannot count.
      def top_level_conjuncts(searchable)
        conjuncts = []
        start = 0
        depth = 0
        between = false

        searchable.to_enum(:scan, /\b(?:AND|OR|BETWEEN)\b|[()]/i).each do
          token = Regexp.last_match(0).upcase
          case token
          when '('
            depth += 1
          when ')'
            return nil if depth.zero?

            depth -= 1
          when 'OR'
            return nil if depth.zero?
          when 'BETWEEN'
            between = true if depth.zero?
          when 'AND'
            next unless depth.zero?

            if between
              between = false
            else
              conjuncts << searchable[start...Regexp.last_match.begin(0)]
              start = Regexp.last_match.end(0)
            end
          end
        end
        return nil unless depth.zero?

        conjuncts << searchable[start..]
        conjuncts
      end

      def skip_parenthesized_sql(source, index)
        depth = 1
        state = :code
        dollar_quote_delimiter = nil

        while index < source.length && depth.positive?
          char = source[index]
          following = source[index + 1]
          case state
          when :dollar_quote
            if source[index, dollar_quote_delimiter.length] == dollar_quote_delimiter
              index += dollar_quote_delimiter.length
              state = :code
              dollar_quote_delimiter = nil
              next
            end
          when :single_quote
            if char == "'" && following == "'"
              index += 2
              next
            end
            state = :code if char == "'"
          when :double_quote
            if char == '"' && following == '"'
              index += 2
              next
            end
            state = :code if char == '"'
          when :backtick
            state = :code if char == '`'
          when :line_comment
            state = :code if char == "\n"
          when :block_comment
            if char == '*' && following == '/'
              state = :code
              index += 2
              next
            end
          else
            if (delimiter = dollar_quote_delimiter_at(source, index))
              state = :dollar_quote
              dollar_quote_delimiter = delimiter
              index += delimiter.length
              next
            elsif char == "'"
              state = :single_quote
            elsif char == '"'
              state = :double_quote
            elsif char == '`'
              state = :backtick
            elsif char == '-' && following == '-'
              state = :line_comment
              index += 2
              next
            elsif char == '/' && following == '*'
              state = :block_comment
              index += 2
              next
            elsif char == '('
              depth += 1
            elsif char == ')'
              depth -= 1
            end
          end
          index += 1
        end

        raise SecurityError, 'Unterminated EXISTS subquery' if depth.positive?

        index
      end

      # Preserve character offsets while hiding SQL tokens in values and comments.
      def mask_sql_literals_and_comments(source)
        masked = source.dup
        state = :code
        dollar_quote_delimiter = nil
        index = 0
        while index < source.length
          char = source[index]
          following = source[index + 1]
          case state
          when :dollar_quote
            if source[index, dollar_quote_delimiter.length] == dollar_quote_delimiter
              masked[index, dollar_quote_delimiter.length] = ' ' * dollar_quote_delimiter.length
              index += dollar_quote_delimiter.length
              state = :code
              dollar_quote_delimiter = nil
              next
            end
            masked[index] = ' '
          when :single_quote
            masked[index] = ' '
            if char == "'" && following == "'"
              masked[index + 1] = ' '
              index += 1
            elsif char == "'"
              state = :code
            end
          when :double_quote
            state = :code if char == '"'
          when :backtick
            state = :code if char == '`'
          when :line_comment
            masked[index] = ' ' unless char == "\n"
            state = :code if char == "\n"
          when :block_comment
            masked[index] = ' '
            if char == '*' && following == '/'
              masked[index + 1] = ' '
              state = :code
              index += 1
            end
          else
            if (delimiter = dollar_quote_delimiter_at(source, index))
              masked[index, delimiter.length] = ' ' * delimiter.length
              state = :dollar_quote
              dollar_quote_delimiter = delimiter
              index += delimiter.length
              next
            elsif char == "'"
              masked[index] = ' '
              state = :single_quote
            elsif char == '"'
              state = :double_quote
            elsif char == '`'
              state = :backtick
            elsif char == '-' && following == '-'
              masked[index] = masked[index + 1] = ' '
              state = :line_comment
              index += 1
            elsif char == '/' && following == '*'
              masked[index] = masked[index + 1] = ' '
              state = :block_comment
              index += 1
            end
          end
          index += 1
        end
        raise SecurityError, 'Unterminated SQL literal or comment' unless %i[code line_comment].include?(state)

        masked
      end

      # PostgreSQL dollar-quote tags follow unquoted identifier rules (without
      # dollar signs); the empty tag is valid too. In particular, $1 is a bind,
      # not the start of a dollar-quoted literal.
      def dollar_quote_delimiter_at(source, index)
        source[index..]&.match(/\A\$(?:[a-zA-Z_][a-zA-Z0-9_]*)?\$/)&.[](0)
      end

      public

      def extract_table_names(sql)
        extract_table_identifiers(sql).map(&:name).uniq
      end

      def extract_table_identifiers(sql)
        source = sql.to_s
        searchable = mask_sql_literals_comments_and_identifier_contents(source)
        tables = []

        searchable.to_enum(:scan, /\bFROM\b\s*(.+?)(?=\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bHAVING\b|\bUNION\b|#{join_clause_pattern}|\z)/im).each do
          match = Regexp.last_match
          clause = source[match.begin(1)...match.end(1)]
          masked_clause = searchable[match.begin(1)...match.end(1)]
          commas = masked_clause.to_enum(:scan, /,/).map { Regexp.last_match.begin(0) }
          [-1, *commas, clause.length].each_cons(2) do |left, right|
            reference = clause[(left + 1)...right]
            table_name = extract_table_reference_identifier(reference)
            raise SecurityError, "Unsupported FROM reference: #{reference.to_s.strip}" unless table_name

            tables << table_name
          end
        end

        searchable.to_enum(:scan, /#{join_clause_pattern}\s*(.+?)(?=\bON\b|\bUSING\b|\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bHAVING\b|\bUNION\b|#{join_clause_pattern}|\z)/im).each do
          match = Regexp.last_match
          reference = source[match.begin(1)...match.end(1)]
          table_name = extract_table_reference_identifier(reference)
          raise SecurityError, "Unsupported JOIN reference: #{reference.to_s.strip}" unless table_name

          tables << table_name
        end

        tables.uniq { |identifier| [identifier.name, identifier.quoted] }
      end

      private

      def mask_sql_literals_comments_and_identifier_contents(source)
        masked = mask_sql_literals_and_comments(source)
        quote = nil
        index = 0
        while index < source.length
          char = source[index]
          if quote
            if char == quote && source[index + 1] == quote
              masked[index] = masked[index + 1] = ' '
              index += 1
            elsif char == quote
              quote = nil
            else
              masked[index] = ' '
            end
          elsif ['"', '`'].include?(char) && masked[index] == char
            quote = char
          end
          index += 1
        end
        masked
      end

      def join_clause_pattern
        /\b(?:INNER\s+|(?:LEFT|RIGHT|FULL)(?:\s+OUTER)?\s+|CROSS\s+|NATURAL\s+(?:(?:LEFT|RIGHT|FULL)(?:\s+OUTER)?\s+)?)?JOIN\b/i
      end

      def extract_table_reference_identifier(reference)
        alias_identifier = '(?:`[^`]+`|"[^"]+"|\'[^\']+\'|[a-zA-Z_][a-zA-Z0-9_]*)'
        table_reference_pattern = /\A(?:`([^`]+)`|"([^"]+)"|'([^']+)'|([a-zA-Z0-9_]+))(?:\s+(?:AS\s+)?#{alias_identifier})?\z/i

        match = reference.to_s.strip.match(table_reference_pattern)
        return unless match

        quoted_name = match.captures[0..2].compact.first
        TableIdentifier.new(name: quoted_name || match[4], quoted: !quoted_name.nil?)
      end
    end
  end
end
