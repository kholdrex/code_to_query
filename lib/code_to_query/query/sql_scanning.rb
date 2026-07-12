# frozen_string_literal: true

module CodeToQuery
  class Query
    # Internal SQL scanning helpers used to enforce table allowlists while
    # preserving quoted literals, identifiers, and comment boundaries.
    class SqlScanner
      def strip_exists_subqueries(sql)
        source = sql.to_s
        searchable = mask_sql_literals_and_comments(source)
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
        searchable = mask_sql_literals_and_comments(source)
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

      private

      def skip_parenthesized_sql(source, index)
        depth = 1
        state = :code

        while index < source.length && depth.positive?
          char = source[index]
          following = source[index + 1]
          case state
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
            if char == "'"
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
        index = 0
        while index < source.length
          char = source[index]
          following = source[index + 1]
          case state
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
            if char == "'"
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

      public

      def extract_table_names(sql)
        tables = []

        sql.scan(/\bFROM\s+(.+?)(?=\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bHAVING\b|\bUNION\b|#{join_clause_pattern}|\z)/im) do |match|
          match.first.split(',').each do |reference|
            table_name = extract_table_reference_name(reference)
            raise SecurityError, "Unsupported FROM reference: #{reference.to_s.strip}" unless table_name

            tables << table_name
          end
        end

        sql.scan(/#{join_clause_pattern}\s+(.+?)(?=\bON\b|\bUSING\b|\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bHAVING\b|\bUNION\b|#{join_clause_pattern}|\z)/im) do |match|
          table_name = extract_table_reference_name(match.first)
          raise SecurityError, "Unsupported JOIN reference: #{match.first.to_s.strip}" unless table_name

          tables << table_name
        end

        tables.uniq
      end

      private

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
