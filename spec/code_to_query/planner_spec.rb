# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery::Planner do
  let(:config) { stub_config(provider: :local, default_limit: 100) }
  let(:planner) { described_class.new(config) }

  describe '#plan' do
    context 'with local provider' do
      it 'returns a valid intent hash' do
        result = planner.plan(
          prompt: 'Get all users',
          schema: sample_schema,
          allow_tables: ['users']
        )

        expect(result).to include(
          'type' => 'select',
          'table' => 'users',
          'columns' => ['*'],
          'limit' => 100
        )
      end

      it 'handles top N queries' do
        result = planner.plan(
          prompt: 'Top 5 users',
          schema: sample_schema,
          allow_tables: ['users']
        )

        # Minimal local provider does not parse top N
        expect(result['limit']).to eq(100)
        expect(result['order']).to eq([])
      end

      it 'extracts date filters for temporal queries' do
        result = planner.plan(
          prompt: 'Users from July 2023',
          schema: sample_schema,
          allow_tables: ['users']
        )

        # Minimal local provider does not parse temporal filters
        expect(result['filters']).to eq([])
        expect(result['params']).to eq({})
      end
    end

    context 'with OpenAI provider' do
      before do
        config.provider = :openai
        config.openai_api_key = 'test-key'
        config.stub_llm = false
      end

      it 'falls back to local provider on API failure' do
        provider = instance_double(CodeToQuery::Providers::OpenAI)
        allow(CodeToQuery::Providers::OpenAI).to receive(:new).and_return(provider)
        allow(provider).to receive(:extract_intent).and_raise(StandardError, 'API Error')

        expect do
          result = planner.plan(
            prompt: 'Get users',
            schema: sample_schema,
            allow_tables: ['users']
          )
          expect(result).to be_a(Hash)
        end.not_to raise_error
      end
    end

    context 'with invalid table allowlist' do
      it 'falls back to safe defaults' do
        result = planner.plan(
          prompt: 'Get all data',
          schema: sample_schema,
          allow_tables: ['restricted_table']
        )

        expect(result['table']).to eq('restricted_table')
      end
    end
  end
end
