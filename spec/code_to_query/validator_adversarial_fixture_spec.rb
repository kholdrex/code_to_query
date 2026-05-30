# frozen_string_literal: true

# rubocop:disable RSpec/LeakyLocalVariable

require 'spec_helper'
require 'yaml'

adversarial_validator_fixture_path = File.expand_path('../fixtures/adversarial/validator.yml', __dir__)
adversarial_validator_cases = YAML.safe_load_file(adversarial_validator_fixture_path, permitted_classes: [], aliases: false).freeze
adversarial_validator_expected_categories = %w[
  literal_smuggling
  policy_bypass
  prompt_injection
  system_table
].freeze

RSpec.describe CodeToQuery::Validator do
  after do
    CodeToQuery.config.policy_adapter = nil
    CodeToQuery.config.policy_adapter_fail_open = false
  end

  adversarial_validator_cases.each do |test_case|
    context "with #{test_case.fetch('category')} fixture #{test_case.fetch('id')}" do
      it 'rejects the intent with the expected reason' do
        policy = test_case['policy']
        policy_adapter = policy && lambda do |_user, **_context|
          {
            allowed_tables: policy.fetch('allowed_tables', []),
            allowed_columns: policy.fetch('allowed_columns', {})
          }
        end

        stub_config(policy_adapter: policy_adapter, policy_adapter_fail_open: false)

        expect do
          described_class.new.validate(
            test_case.fetch('intent'),
            allow_tables: test_case.fetch('allow_tables', nil)
          )
        end.to raise_error(ArgumentError, /#{Regexp.escape(test_case.fetch('reason'))}/)
      end
    end
  end

  it 'keeps every fixture categorized with an expected rejection reason' do
    categories = adversarial_validator_cases.map { |test_case| test_case.fetch('category') }.uniq

    expect(categories).to match_array(adversarial_validator_expected_categories)
    expect(adversarial_validator_cases).to all(include('id', 'category', 'intent', 'reason'))
  end
end
# rubocop:enable RSpec/LeakyLocalVariable
