# frozen_string_literal: true

require 'spec_helper'
require 'code_to_query/provider_evaluation'

RSpec.describe CodeToQuery::ProviderEvaluation do
  let(:fixture_path) { CodeToQuery::ProviderEvaluation::DEFAULT_FIXTURE_PATH }
  let(:config) { stub_config(default_limit: 100, provider: :local) }

  describe '.load_file' do
    it 'loads fixture cases from YAML' do
      evaluation = described_class.load_file(fixture_path, config: config)

      expect(evaluation.cases.length).to eq(4)
    end
  end

  describe '#run' do
    it 'evaluates local provider cases against deterministic safety expectations' do
      evaluation = described_class.load_file(fixture_path, config: config).run

      expect(evaluation).to be_passed
      expect(evaluation.summary).to include(total: 4, passed: 4, failed: 0, success_rate: 1.0)
      expect(evaluation.results.map(&:intent)).to all(include('params' => be_a(Hash)))
    end

    it 'reports fixture expectation failures without raising' do
      data = {
        'schema' => sample_schema,
        'cases' => [
          {
            'name' => 'wrong table expectation',
            'prompt' => 'Show orders',
            'allow_tables' => ['orders'],
            'expect' => {
              'table' => 'users',
              'params_hash' => true
            }
          }
        ]
      }

      evaluation = described_class.new(data, config: config).run

      expect(evaluation).not_to be_passed
      expect(evaluation.summary).to include(total: 1, passed: 0, failed: 1)
      expect(evaluation.results.first.failures).to include('expected table "users", got "orders"')
    end
  end

  describe '#initialize' do
    it 'rejects malformed cases during setup' do
      data = { 'cases' => [{ 'name' => 'missing fields' }] }

      expect { described_class.new(data, config: config) }.to raise_error(ArgumentError, /missing: prompt, allow_tables, expect/)
    end

    it 'rejects empty fixture suites' do
      expect { described_class.new({ 'cases' => [] }, config: config) }
        .to raise_error(ArgumentError, /requires at least one case/)
    end

    it 'rejects non-hash expectations' do
      data = {
        'cases' => [
          {
            'name' => 'invalid expectation',
            'prompt' => 'Show users',
            'allow_tables' => ['users'],
            'expect' => nil
          }
        ]
      }

      expect { described_class.new(data, config: config) }
        .to raise_error(ArgumentError, /expect must be a Hash/)
    end
  end
end
