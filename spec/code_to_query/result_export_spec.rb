# frozen_string_literal: true

require 'spec_helper'
require 'json'
require 'csv'

RSpec.describe CodeToQuery::ResultExport do
  let(:result_like_class) { Struct.new(:columns, :rows, keyword_init: true) }

  describe '.to_a' do
    it 'converts an ActiveRecord::Result-like object with columns and array rows to hashes' do
      result = result_like_class.new(columns: %w[id email], rows: [[1, 'ada@example.test'], [2, 'grace@example.test']])

      expect(described_class.to_a(result)).to eq(
        [
          { 'id' => 1, 'email' => 'ada@example.test' },
          { 'id' => 2, 'email' => 'grace@example.test' }
        ]
      )
    end

    it 'uses existing hash rows from to_a without executing queries' do
      result = Class.new do
        def to_a
          [{ 'id' => 1, 'email' => 'ada@example.test' }]
        end

        def run
          raise 'should not run queries'
        end

        def execute
          raise 'should not execute queries'
        end
      end.new

      expect(described_class.to_a(result)).to eq([{ 'id' => 1, 'email' => 'ada@example.test' }])
    end

    it 'stringifies symbol keys in hash rows' do
      result = [{ id: 1, email: 'ada@example.test' }]

      expect(described_class.to_a(result)).to eq([{ 'id' => 1, 'email' => 'ada@example.test' }])
    end

    it 'handles empty results' do
      result = result_like_class.new(columns: %w[id email], rows: [])

      expect(described_class.to_a(result)).to eq([])
    end
  end

  describe '.to_json' do
    it 'generates JSON from shaped rows' do
      result = result_like_class.new(columns: %w[id email], rows: [[1, 'ada@example.test']])

      expect(JSON.parse(described_class.to_json(result))).to eq(
        [
          { 'id' => 1, 'email' => 'ada@example.test' }
        ]
      )
    end
  end

  describe '.to_csv' do
    it 'preserves result column order for CSV output' do
      result = result_like_class.new(columns: %w[email id], rows: [['ada@example.test', 1]])

      csv = described_class.to_csv(result)

      expect(CSV.parse(csv)).to eq(
        [
          %w[email id],
          ['ada@example.test', '1']
        ]
      )
    end

    it 'includes headers for empty column-and-row results' do
      result = result_like_class.new(columns: %w[id email], rows: [])

      expect(CSV.parse(described_class.to_csv(result))).to eq([%w[id email]])
    end

    it 'infers headers from to_a hash rows when columns are unavailable' do
      result = [{ 'id' => 1, 'email' => 'ada@example.test' }]

      expect(CSV.parse(described_class.to_csv(result))).to eq(
        [
          %w[id email],
          ['1', 'ada@example.test']
        ]
      )
    end

    it 'includes heterogeneous hash row keys in first-seen order' do
      result = [
        { id: 1, email: 'ada@example.test' },
        { id: 2, name: 'Grace Hopper', email: 'grace@example.test' },
        { department: 'engineering', id: 3 }
      ]

      expect(CSV.parse(described_class.to_csv(result))).to eq(
        [
          %w[id email name department],
          ['1', 'ada@example.test', nil, nil],
          ['2', 'grace@example.test', 'Grace Hopper', nil],
          ['3', nil, nil, 'engineering']
        ]
      )
    end
  end
end
