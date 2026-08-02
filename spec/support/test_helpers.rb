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

  # Specs share the production singleton, so restore every setting after each example.
  config.around do |example|
    singleton_config = CodeToQuery::Configuration.instance
    original_state = singleton_config.instance_variables.to_h do |variable|
      [variable, singleton_config.instance_variable_get(variable)]
    end

    example.run
  ensure
    if singleton_config && original_state
      (singleton_config.instance_variables - original_state.keys).each do |variable|
        singleton_config.remove_instance_variable(variable)
      end
      original_state.each do |variable, value|
        singleton_config.instance_variable_set(variable, value)
      end
    end
  end
end
