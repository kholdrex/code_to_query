# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery do
  describe '.configure' do
    it 'yields the configuration instance' do
      expect { |b| described_class.configure(&b) }.to yield_with_args(CodeToQuery::Configuration.instance)
    end

    it 'allows setting configuration options' do
      described_class.configure do |config|
        config.default_limit = 200
        config.adapter = :mysql
      end

      expect(described_class.config.default_limit).to eq(200)
      expect(described_class.config.adapter).to eq(:mysql)
    end
  end

  describe '.ask' do
    before do
      stub_config(stub_llm: true, provider: :local)
    end

    it 'returns a Query object' do
      query = described_class.ask(prompt: 'Get users', allow_tables: ['users'])
      expect(query).to be_a(CodeToQuery::Query)
    end

    it 'passes parameters to the planner' do
      planner = instance_double(CodeToQuery::Planner)
      allow(CodeToQuery::Planner).to receive(:new).and_return(planner)
      allow(planner).to receive(:plan).and_return(sample_intent)

      described_class.ask(prompt: 'Get users', schema: sample_schema, allow_tables: ['users'])

      expect(planner).to have_received(:plan).with(
        prompt: 'Get users',
        schema: sample_schema,
        allow_tables: ['users']
      )
    end
  end
end
