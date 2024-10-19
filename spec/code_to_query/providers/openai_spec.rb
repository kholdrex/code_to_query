# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery::Providers::OpenAI do
  let(:config) { stub_config(openai_api_key: 'test-key', openai_model: 'gpt-4') }
  let(:provider) { described_class.new(config) }

  describe '#extract_intent' do
    let(:mock_response) do
      {
        'choices' => [
          {
            'message' => {
              'function_call' => {
                'arguments' => JSON.generate({
                                               'type' => 'select',
                                               'table' => 'users',
                                               'columns' => ['*'],
                                               'filters' => [],
                                               'order' => [],
                                               'limit' => 100,
                                               'params' => {}
                                             })
              }
            }
          }
        ]
      }
    end

    it 'makes API request and returns parsed intent' do
      allow_any_instance_of(Net::HTTP).to receive(:request).and_return(
        double(is_a?: true, body: JSON.generate(mock_response))
      )

      result = provider.extract_intent(
        prompt: 'Get users',
        schema: sample_schema,
        allow_tables: ['users']
      )

      expect(result).to include(
        'type' => 'select',
        'table' => 'users',
        'columns' => ['*']
      )
    end

    it 'validates table allowlist' do
      allow_any_instance_of(Net::HTTP).to receive(:request).and_return(
        double(is_a?: true, body: JSON.generate({
                                                  'choices' => [
                                                    {
                                                      'message' => {
                                                        'function_call' => {
                                                          'arguments' => JSON.generate({
                                                                                         'type' => 'select',
                                                                                         'table' => 'restricted_table',
                                                                                         'columns' => ['*'],
                                                                                         'limit' => 100
                                                                                       })
                                                        }
                                                      }
                                                    }
                                                  ]
                                                }))
      )

      expect do
        provider.extract_intent(
          prompt: 'Get data',
          schema: sample_schema,
          allow_tables: ['users']
        )
      end.to raise_error(ArgumentError, /not in allowlist/)
    end

    it 'handles API errors gracefully' do
      allow_any_instance_of(Net::HTTP).to receive(:request).and_return(
        double(is_a?: false, code: '500', message: 'Internal Server Error')
      )

      expect do
        provider.extract_intent(
          prompt: 'Get users',
          schema: sample_schema,
          allow_tables: ['users']
        )
      end.to raise_error(/OpenAI API error/)
    end
  end
end
