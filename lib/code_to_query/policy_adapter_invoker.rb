# frozen_string_literal: true

module CodeToQuery
  # Invokes policy adapters once, selecting a compatible keyword or positional
  # context shape before invocation so adapter exceptions can never alter dispatch.
  class PolicyAdapterInvoker
    class << self
      def call(adapter, current_user, table:, intent: nil)
        context = available_context(table: table, intent: intent)
        parameters = callable_parameters(adapter)
        keywords = supported_keywords(parameters, context)
        return adapter.call(current_user, **keywords) unless keywords.empty?
        return adapter.call(current_user, context) if accepts_positional_context?(parameters)

        adapter.call(current_user)
      end

      private

      def available_context(table:, intent:)
        { table: table }.tap do |context|
          context[:intent] = intent unless intent.nil?
        end
      end

      def supported_keywords(parameters, context)
        return context if parameters.any? { |type, _name| type == :keyrest }

        keyword_names = parameters.filter_map do |type, name|
          name if %i[key keyreq].include?(type)
        end
        context.slice(*keyword_names)
      end

      def accepts_positional_context?(parameters)
        return true if parameters.any? { |type, _name| type == :rest }

        parameters.count { |type, _name| %i[req opt].include?(type) } >= 2
      end

      def callable_parameters(adapter)
        return adapter.parameters if adapter.is_a?(Proc) || adapter.is_a?(Method)

        adapter.method(:call).parameters
      end
    end
  end
end
