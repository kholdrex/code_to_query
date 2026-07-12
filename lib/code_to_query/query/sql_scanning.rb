# frozen_string_literal: true

module CodeToQuery
  class Query
    # Internal SQL scanning helpers used to enforce table allowlists while
    # preserving quoted literals, identifiers, and comment boundaries.
    class SqlScanner
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
        searchable = mask_sql_literals_and_comments(sql.to_s)
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

      # Identify binds used as values of the expected qualified policy column.
      # A placeholder elsewhere in the EXISTS body does not enforce policy.
      def policy_predicate_bind_numbers(sql, table, column, question_bind_number: nil)
        return [] if table.to_s.empty? || column.to_s.empty?

        searchable = mask_sql_literals_and_comments(sql.to_s)
        where_match = /\bWHERE\b/i.match(searchable)
        return [] unless where_match

        conjuncts = top_level_conjuncts(searchable[where_match.end(0)..])
        return [] unless conjuncts

        identifier = lambda do |value|
          escaped = Regexp.escape(value.to_s)
          unquoted = "(?<![A-Za-z0-9_$])#{escaped}(?![A-Za-z0-9_$])"
          "(?:\"#{escaped}\"|`#{escaped}`|#{unquoted})"
        end
        qualified = "#{identifier.call(table)}\\s*\\.\\s*#{identifier.call(column)}"
        wrappers = ->(predicate) { /\A\s*\(*\s*#{predicate}\s*\)*\s*\z/i }
        question_pattern = wrappers.call("#{qualified}\\s*(?:=\\s*\\?|BETWEEN\\s*\\?\\s+AND\\s*\\?)")
        if question_bind_number && conjuncts.any? { |conjunct| conjunct.match?(question_pattern) }
          return [question_bind_number]
        end

        pattern = wrappers.call("#{qualified}\\s*(?:=\\s*\\$(\\d+)|BETWEEN\\s*\\$(\\d+)\\s+AND\\s*\\$(\\d+))")
        conjuncts.flat_map do |conjunct|
          match = pattern.match(conjunct)
          match ? match.captures.compact.map(&:to_i) : []
        end.uniq
      end

      private

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
            table_name = extract_table_reference_name(reference)
            raise SecurityError, "Unsupported FROM reference: #{reference.to_s.strip}" unless table_name

            tables << table_name
          end
        end

        searchable.to_enum(:scan, /#{join_clause_pattern}\s*(.+?)(?=\bON\b|\bUSING\b|\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bHAVING\b|\bUNION\b|#{join_clause_pattern}|\z)/im).each do
          match = Regexp.last_match
          reference = source[match.begin(1)...match.end(1)]
          table_name = extract_table_reference_name(reference)
          raise SecurityError, "Unsupported JOIN reference: #{reference.to_s.strip}" unless table_name

          tables << table_name
        end

        tables.uniq
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

      def extract_table_reference_name(reference)
        identifier = '(?:`[^`]+`|"[^"]+"|\'[^\']+\'|[a-zA-Z0-9_]+)'
        alias_identifier = '(?:`[^`]+`|"[^"]+"|\'[^\']+\'|[a-zA-Z_][a-zA-Z0-9_]*)'
        table_reference_pattern = /\A(?:#{identifier}\.)*(?:`([^`]+)`|"([^"]+)"|'([^']+)'|([a-zA-Z0-9_]+))(?:\s+(?:AS\s+)?#{alias_identifier})?\z/i

        match = reference.to_s.strip.match(table_reference_pattern)
        captures = match&.captures
        captures&.compact&.first
      end
    end
  end
end
