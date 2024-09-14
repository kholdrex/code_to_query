# frozen_string_literal: true

begin
  require 'active_record'
  require 'timeout'
rescue LoadError
end

module CodeToQuery
  class Runner
    DEFAULT_TIMEOUT = 30
    MAX_ROWS_RETURNED = 10_000

    def initialize(config)
      @config = config
    end

    def run(sql:, binds: [])
      validate_execution_context!

      result = execute_with_timeout(sql, binds)
      format_result(result)
    rescue StandardError => e
      handle_execution_error(e, sql)
    end

    private

    def validate_execution_context!
      unless defined?(ActiveRecord::Base) && ActiveRecord::Base.connected?
        raise ConnectionError, 'ActiveRecord not available or not connected'
      end

      return unless @config.readonly_role && !supports_readonly_role?

      CodeToQuery.config.logger.warn('[code_to_query] Readonly role specified but not supported in this Rails version')
    end

    def execute_with_timeout(sql, binds)
      timeout = @config.query_timeout || DEFAULT_TIMEOUT

      Timeout.timeout(timeout) do
        if @config.readonly_role && supports_readonly_role?
          execute_with_readonly_role(sql, binds)
        else
          execute_with_regular_connection(sql, binds)
        end
      end
    rescue Timeout::Error
      raise ExecutionError, "Query timed out after #{timeout} seconds"
    end

    def execute_with_readonly_role(sql, binds)
      ActiveRecord::Base.connected_to(role: @config.readonly_role) do
        connection = ActiveRecord::Base.connection

        verify_readonly_connection(connection)

        connection.exec_query(sql, 'CodeToQuery', binds)
      end
    end

    def execute_with_regular_connection(sql, binds)
      connection = ActiveRecord::Base.connection

      set_session_readonly(connection)

      begin
        connection.exec_query(sql, 'CodeToQuery', binds)
      ensure
        reset_session_readonly(connection) if @config.reset_session_after_query
      end
    end

    def verify_readonly_connection(connection)
      case @config.adapter
      when :postgres, :postgresql
        result = connection.execute('SHOW transaction_read_only')
        readonly = result.first['transaction_read_only']
        unless readonly == 'on'
          CodeToQuery.config.logger.warn("[code_to_query] Warning: Connection may not be read-only (transaction_read_only: #{readonly})")
        end
      when :mysql
        # MySQL doesn't have a direct equivalent, but we can check user privileges
        # This is more complex and would require additional setup
      end
    rescue StandardError => e
      CodeToQuery.config.logger.warn("[code_to_query] Could not verify readonly status: #{e.message}")
    end

    def set_session_readonly(connection)
      return unless @config.force_readonly_session

      case @config.adapter
      when :postgres, :postgresql
        connection.execute('SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY')
      when :mysql
        connection.execute('SET SESSION TRANSACTION READ ONLY')
      end
    rescue StandardError => e
      CodeToQuery.config.logger.warn("[code_to_query] Could not set session to readonly: #{e.message}")
    end

    def reset_session_readonly(connection)
      case @config.adapter
      when :postgres, :postgresql
        connection.execute('SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE')
      when :mysql
        connection.execute('SET SESSION TRANSACTION READ WRITE')
      end
    rescue StandardError => e
      CodeToQuery.config.logger.warn("[code_to_query] Could not reset session readonly state: #{e.message}")
    end

    def format_result(result)
      return stub_result unless result

      # Limit result size for safety
      if result.respond_to?(:rows) && result.rows.length > MAX_ROWS_RETURNED
        CodeToQuery.config.logger.warn("[code_to_query] Result truncated to #{MAX_ROWS_RETURNED} rows")
        limited_rows = result.rows.first(MAX_ROWS_RETURNED)

        if defined?(ActiveRecord::Result)
          ActiveRecord::Result.new(result.columns, limited_rows, result.column_types)
        else
          { columns: result.columns, rows: limited_rows, truncated: true }
        end
      else
        result
      end
    end

    def handle_execution_error(error, sql)
      error_info = {
        type: error.class.name,
        message: error.message,
        sql_preview: sql[0..100] + (sql.length > 100 ? '...' : ''),
        timestamp: Time.now
      }

      case error
      when ActiveRecord::StatementInvalid
        log_execution_error('Database error', error_info)
        raise ExecutionError, "Database error: #{error.message}"

      when ActiveRecord::RecordNotFound
        log_execution_error('Record not found', error_info)
        raise ExecutionError, "Query returned no results: #{error.message}"

      when Timeout::Error
        log_execution_error('Query timeout', error_info)
        raise ExecutionError, 'Query execution timed out'

      when ConnectionError, ExecutionError
        log_execution_error('Execution error', error_info)
        raise error

      else
        log_execution_error('Unexpected error', error_info)
        raise ExecutionError, "Unexpected error during query execution: #{error.message}"
      end
    end

    def log_execution_error(category, error_info)
      if defined?(Rails) && Rails.logger
        Rails.logger.error "[code_to_query] #{category}: #{error_info}"
      else
        CodeToQuery.config.logger.warn("[code_to_query] #{category}: #{error_info[:message]}")
      end
    end

    def supports_readonly_role?
      ActiveRecord.respond_to?(:connected_to) && ActiveRecord::Base.respond_to?(:connected_to)
    end

    def stub_result
      if defined?(ActiveRecord::Result)
        ActiveRecord::Result.new([], [])
      else
        { columns: [], rows: [], message: 'No database connection available' }
      end
    end
  end

  # Custom exception classes for better error handling
  class ConnectionError < StandardError; end
  class ExecutionError < StandardError; end
end
