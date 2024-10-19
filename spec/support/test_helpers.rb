# frozen_string_literal: true

module TestHelpers
  def stub_config(**options)
    config = CodeToQuery::Configuration.instance
    # Ensure extended accessors exist for tests
    CodeToQuery::BackCompat.ensure_extended_config!(config) if defined?(CodeToQuery::BackCompat)
    # Route logger to stderr with sync so specs expecting stderr output still pass
    logger = Logger.new($stderr)
    logger.level = Logger::WARN
    logger.formatter = proc { |severity, _datetime, _progname, msg|
      $stderr.sync = true
      "#{severity}: #{msg}\n"
    }
    config.logger = logger
    options.each { |key, value| config.send("#{key}=", value) }
    config
  end

  def sample_schema
    {
      'tables' => [
        {
          'name' => 'users',
          'columns' => [
            { 'name' => 'id', 'sql_type' => 'integer', 'type' => 'integer' },
            { 'name' => 'email', 'sql_type' => 'varchar(255)', 'type' => 'string' },
            { 'name' => 'created_at', 'sql_type' => 'timestamp', 'type' => 'datetime' }
          ]
        },
        {
          'name' => 'orders',
          'columns' => [
            { 'name' => 'id', 'sql_type' => 'integer', 'type' => 'integer' },
            { 'name' => 'user_id', 'sql_type' => 'integer', 'type' => 'integer' },
            { 'name' => 'total', 'sql_type' => 'decimal(10,2)', 'type' => 'decimal' },
            { 'name' => 'created_at', 'sql_type' => 'timestamp', 'type' => 'datetime' }
          ]
        }
      ]
    }
  end

  def sample_intent
    {
      'type' => 'select',
      'table' => 'users',
      'columns' => ['*'],
      'filters' => [],
      'order' => [],
      'limit' => 100,
      'params' => {}
    }
  end
end

RSpec.configure do |config|
  config.include TestHelpers
end
