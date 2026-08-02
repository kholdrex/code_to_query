# frozen_string_literal: true

module CodeToQuery
  # Database identifier folding is deliberately narrower than Ruby's Unicode
  # case folding. The supported adapters fold ASCII identifier letters only;
  # non-ASCII codepoints remain distinct catalog characters.
  module IdentifierSemantics
    module_function

    def ascii_fold(value)
      value.to_s.tr('A-Z', 'a-z')
    end

    def ascii_case_insensitive?(left, right)
      ascii_fold(left) == ascii_fold(right)
    end

    def ascii_case_insensitive_pattern(value)
      value.to_s.each_char.map do |character|
        if character.match?(/\A[A-Za-z]\z/)
          lower = character.tr('A-Z', 'a-z')
          "[#{lower}#{lower.tr('a-z', 'A-Z')}]"
        else
          Regexp.escape(character)
        end
      end.join
    end
  end
end
