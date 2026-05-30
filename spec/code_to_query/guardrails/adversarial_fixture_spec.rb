# frozen_string_literal: true

# rubocop:disable RSpec/LeakyLocalVariable

require 'spec_helper'
require 'yaml'

adversarial_sql_fixture_path = File.expand_path('../../fixtures/adversarial/sql_linter.yml', __dir__)
adversarial_sql_cases = YAML.safe_load_file(adversarial_sql_fixture_path, permitted_classes: [], aliases: false).freeze
adversarial_sql_adapters = %w[postgres mysql sqlite].freeze
adversarial_sql_allowed_adapters = (adversarial_sql_adapters + %w[any]).freeze
adversarial_sql_expected_categories = %w[
  comment_obfuscation
  dangerous_function
  encoded_payload
  literal_smuggling
  prompt_injection
  stacked_statement
  system_table
  union
].freeze

RSpec.describe 'Adversarial SQL fixture corpus' do
  adversarial_sql_cases.each do |test_case|
    adapter = test_case.fetch('adapter')
    adapters = adapter == 'any' ? adversarial_sql_adapters : [adapter]

    adapters.each do |adapter_name|
      context "with #{test_case.fetch('category')} fixture #{test_case.fetch('id')} on #{adapter_name}" do
        it 'rejects the SQL with the expected reason' do
          linter = CodeToQuery::Guardrails::SqlLinter.new(
            stub_config(adapter: adapter_name.to_sym, max_limit: 1000, max_joins: 3, policy_adapter: nil),
            allow_tables: %w[users orders]
          )

          expect { linter.check!(test_case.fetch('sql')) }
            .to raise_error(SecurityError, /#{Regexp.escape(test_case.fetch('reason'))}/)
        end
      end
    end
  end

  it 'keeps every fixture categorized with an expected rejection reason' do
    categories = adversarial_sql_cases.map { |test_case| test_case.fetch('category') }.uniq
    adapters = adversarial_sql_cases.map { |test_case| test_case.fetch('adapter') }.uniq

    expect(categories).to match_array(adversarial_sql_expected_categories)
    expect(adapters - adversarial_sql_allowed_adapters).to be_empty
    expect(adversarial_sql_cases).to all(include('id', 'category', 'adapter', 'sql', 'reason'))
  end
end
# rubocop:enable RSpec/LeakyLocalVariable
