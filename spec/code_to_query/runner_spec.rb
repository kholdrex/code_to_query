# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery::Runner do
  let(:config) { stub_config(adapter: :postgres, query_timeout: 30) }
  let(:runner) { described_class.new(config) }

  describe '#run' do
    context 'when ActiveRecord is not available' do
      before do
        allow(runner).to receive(:validate_execution_context!).and_raise(
          CodeToQuery::ConnectionError, 'ActiveRecord not available or not connected'
        )
      end

      it 'raises ConnectionError' do
        expect { runner.run(sql: 'SELECT 1', binds: []) }.to raise_error(
          CodeToQuery::ConnectionError, /ActiveRecord not available/
        )
      end
    end

    context 'when query times out' do
      before do
        allow(runner).to receive(:validate_execution_context!)
        allow(runner).to receive(:execute_with_timeout).and_raise(
          CodeToQuery::ExecutionError, 'Query timed out after 30 seconds'
        )
      end

      it 'raises ExecutionError with timeout message' do
        expect { runner.run(sql: 'SELECT 1', binds: []) }.to raise_error(
          CodeToQuery::ExecutionError, /timed out/
        )
      end
    end

    context 'when query executes successfully' do
      let(:mock_result) do
        double('Result', columns: %w[id name], rows: [[1, 'Alice'], [2, 'Bob']])
      end

      before do
        allow(runner).to receive(:validate_execution_context!)
        allow(runner).to receive(:execute_with_timeout).and_return(mock_result)
      end

      it 'returns the result' do
        result = runner.run(sql: 'SELECT id, name FROM users', binds: [])
        expect(result).to eq(mock_result)
      end
    end

    context 'when result exceeds MAX_ROWS_RETURNED' do
      let(:large_rows) { (1..15_000).map { |i| [i, "User#{i}"] } }
      let(:mock_result) do
        double('Result',
               columns: %w[id name],
               rows: large_rows,
               column_types: {})
      end

      before do
        allow(runner).to receive(:validate_execution_context!)
        allow(runner).to receive(:execute_with_timeout).and_return(mock_result)
        stub_const('CodeToQuery::Runner::MAX_ROWS_RETURNED', 10_000)
      end

      it 'truncates results to MAX_ROWS_RETURNED' do
        result = runner.run(sql: 'SELECT * FROM users', binds: [])

        if result.respond_to?(:rows)
          expect(result.rows.length).to be <= 10_000
        else
          expect(result[:rows].length).to be <= 10_000
          expect(result[:truncated]).to be true
        end
      end
    end
  end

  describe '#validate_execution_context!' do
    context 'when ActiveRecord is connected' do
      before do
        ar_base = Class.new do
          def self.connected?
            true
          end
        end
        stub_const('ActiveRecord::Base', ar_base)
      end

      it 'does not raise an error' do
        expect { runner.send(:validate_execution_context!) }.not_to raise_error
      end
    end
  end

  describe '#format_result' do
    context 'with nil result' do
      it 'returns a stub result' do
        result = runner.send(:format_result, nil)

        if result.respond_to?(:columns)
          expect(result.columns).to eq([])
        else
          expect(result[:columns]).to eq([])
        end
      end
    end

    context 'with normal result' do
      let(:mock_result) do
        double('Result', columns: ['id'], rows: [[1], [2]])
      end

      before do
        allow(mock_result).to receive(:respond_to?).with(:rows).and_return(true)
      end

      it 'returns the result unchanged' do
        result = runner.send(:format_result, mock_result)
        expect(result).to eq(mock_result)
      end
    end
  end

  describe '#handle_execution_error' do
    let(:sql) { 'SELECT * FROM users WHERE id = 1' }

    context 'with Timeout::Error' do
      it 'raises ExecutionError' do
        error = Timeout::Error.new('execution expired')
        expect { runner.send(:handle_execution_error, error, sql) }.to raise_error(
          CodeToQuery::ExecutionError, /timed out/
        )
      end
    end

    context 'with unexpected error' do
      it 'raises ExecutionError with message' do
        error = StandardError.new('something went wrong')
        expect { runner.send(:handle_execution_error, error, sql) }.to raise_error(
          CodeToQuery::ExecutionError, /Unexpected error/
        )
      end
    end

    context 'with ConnectionError' do
      it 're-raises the same error' do
        error = CodeToQuery::ConnectionError.new('no connection')
        expect { runner.send(:handle_execution_error, error, sql) }.to raise_error(
          CodeToQuery::ConnectionError, 'no connection'
        )
      end
    end

    context 'with ExecutionError' do
      it 're-raises the same error' do
        error = CodeToQuery::ExecutionError.new('execution failed')
        expect { runner.send(:handle_execution_error, error, sql) }.to raise_error(
          CodeToQuery::ExecutionError, 'execution failed'
        )
      end
    end
  end

  describe '#supports_readonly_role?' do
    context 'when ActiveRecord supports connected_to' do
      before do
        stub_const('ActiveRecord', Module.new)
        stub_const('ActiveRecord::Base', Class.new)
        allow(ActiveRecord).to receive(:respond_to?).with(:connected_to).and_return(true)
        allow(ActiveRecord::Base).to receive(:respond_to?).with(:connected_to).and_return(true)
      end

      it 'returns true' do
        expect(runner.send(:supports_readonly_role?)).to be true
      end
    end
  end

  describe '#stub_result' do
    it 'returns an empty result structure' do
      result = runner.send(:stub_result)

      if result.respond_to?(:columns)
        expect(result.columns).to eq([])
        expect(result.rows).to eq([])
      else
        expect(result[:columns]).to eq([])
        expect(result[:rows]).to eq([])
      end
    end
  end

  describe '#set_session_readonly' do
    let(:connection) { double('Connection') }

    context 'when force_readonly_session is false' do
      before do
        config.force_readonly_session = false
      end

      it 'does not execute any SQL' do
        expect { runner.send(:set_session_readonly, connection) }.not_to raise_error
      end
    end

    context 'when force_readonly_session is true with postgres' do
      before do
        config.force_readonly_session = true
        config.adapter = :postgres
        allow(connection).to receive(:execute)
      end

      it 'sets session to readonly' do
        runner.send(:set_session_readonly, connection)
        expect(connection).to have_received(:execute).with('SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY')
      end
    end

    context 'when force_readonly_session is true with mysql' do
      before do
        config.force_readonly_session = true
        config.adapter = :mysql
        allow(connection).to receive(:execute)
      end

      it 'sets session to readonly' do
        runner.send(:set_session_readonly, connection)
        expect(connection).to have_received(:execute).with('SET SESSION TRANSACTION READ ONLY')
      end
    end
  end

  describe '#reset_session_readonly' do
    let(:connection) { double('Connection') }

    context 'with postgres adapter' do
      before do
        config.adapter = :postgres
        allow(connection).to receive(:execute)
      end

      it 'resets session to read-write' do
        runner.send(:reset_session_readonly, connection)
        expect(connection).to have_received(:execute).with('SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE')
      end
    end

    context 'with mysql adapter' do
      before do
        config.adapter = :mysql
        allow(connection).to receive(:execute)
      end

      it 'resets session to read-write' do
        runner.send(:reset_session_readonly, connection)
        expect(connection).to have_received(:execute).with('SET SESSION TRANSACTION READ WRITE')
      end
    end
  end
end
