# frozen_string_literal: true

begin
  require 'pundit'
rescue LoadError
end

module CodeToQuery
  module Policies
    class PunditAdapter
      def call(current_user, table:, intent: nil) # rubocop:disable Lint/UnusedMethodArgument
        return {} unless defined?(Pundit)

        info = {
          enforced_predicates: inferred_tenant_predicates(current_user, table),
          allowed_tables: [],
          allowed_columns: {}
        }

        model = infer_model_for_table(table)
        if model
          begin
            Pundit.policy_scope!(current_user, model)
            info[:allowed_tables] << table
          rescue StandardError
          end

          policy = Pundit.policy(current_user, model)
          if policy.respond_to?(:permitted_columns)
            cols = Array(policy.permitted_columns).map(&:to_s)
            info[:allowed_columns][table] = cols if cols.any?
          end
        end

        info
      end

      private

      def infer_model_for_table(table_name)
        return nil unless defined?(ActiveRecord::Base)
        return nil unless table_name

        candidates = [
          table_name.to_s.singularize.camelize,
          table_name.to_s.camelize,
          table_name.to_s.singularize.camelize.gsub(/s$/, '')
        ]

        candidates.each do |klass|
          k = klass.constantize
          return k if k < ActiveRecord::Base && k.table_name == table_name
        rescue NameError
          next
        end
        nil
      end

      def inferred_tenant_predicates(current_user, _table)
        return {} unless current_user
        return {} unless current_user.respond_to?(:company_id)

        if current_user.company_id
          { company_id: current_user.company_id }
        else
          {}
        end
      end
    end
  end
end
