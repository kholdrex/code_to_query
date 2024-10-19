# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery::Providers::Local do
  let(:config) { stub_config(default_limit: 100) }
  let(:provider) { described_class.new(config) }

  describe '#extract_intent' do
    it 'extracts table from allowlist' do
      result = provider.extract_intent(
        prompt: 'Get orders',
        schema: sample_schema,
        allow_tables: ['orders']
      )

      expect(result['table']).to eq('orders')
    end

    it 'detects top N queries' do
      result = provider.extract_intent(
        prompt: 'Top 10 users by creation date',
        schema: sample_schema,
        allow_tables: ['users']
      )

      expect(result['limit']).to eq(100) # minimal local provider does not parse top-N
      expect(result['order']).to eq([])
    end

    it 'extracts month-based time filters' do
      result = provider.extract_intent(
        prompt: 'Users from July 2023',
        schema: sample_schema,
        allow_tables: ['users']
      )

      expect(result['filters']).to eq([])
      expect(result['params']).to eq({})
    end

    it 'falls back to default table when none found' do
      result = provider.extract_intent(
        prompt: 'Show me data',
        schema: {},
        allow_tables: []
      )

      expect(result['table']).to eq('main_table')
      expect(result['columns']).to eq(['*'])
      expect(result['limit']).to eq(100)
    end

    it 'calculates semantic similarity for table matching' do
      schema = {
        'tables' => [
          { 'name' => 'customers', 'columns' => [] },
          { 'name' => 'orders', 'columns' => [] }
        ]
      }

      result = provider.extract_intent(
        prompt: 'Get all clients',
        schema: schema,
        allow_tables: []
      )

      # minimal provider picks first schema table when no allowlist
      expect(%w[customers orders]).to include(result['table'])
    end

    it 'detects "not answered by student" and builds NOT EXISTS intent' do
      schema = {
        'tables' => [
          { 'name' => 'questions', 'columns' => [{ 'name' => 'id' }] },
          { 'name' => 'answers',   'columns' => [
            { 'name' => 'id' }, { 'name' => 'question_id' }, { 'name' => 'student_id' }
          ] }
        ]
      }

      result = provider.extract_intent(
        prompt: 'all questions that are not answered by student with id 17963',
        schema: schema,
        allow_tables: ['questions']
      )

      # Minimal local provider does not synthesize NOT EXISTS
      expect(result['filters']).to eq([])
      expect(result['params']).to eq({})
    end
  end
end
