# frozen_string_literal: true

# Context::Pack is a serializable bundle of schema, associations, glossary,
# join paths, policies, and hints used by the planner.

require 'json'

module CodeToQuery
  module Context
    class Pack
      attr_reader :schema, :models, :glossary, :policies, :hints

      def initialize(schema:, models:, glossary:, policies:, hints:)
        @schema = schema
        @models = models
        @glossary = glossary
        @policies = policies
        @hints = hints
      end

      def to_h
        {
          schema: @schema,
          models: @models,
          glossary: @glossary,
          policies: @policies,
          hints: @hints
        }
      end

      def to_json(*args)
        JSON.pretty_generate(to_h, *args)
      end
    end
  end
end
