# frozen_string_literal: true

require 'fileutils'
require 'json'
require 'logger'
require 'rake'
require 'tmpdir'

RSpec.describe 'CodeToQuery context rake tasks' do
  let(:context_path) { File.join(tmpdir, 'context-pack.json') }
  let(:tmpdir) { Dir.mktmpdir('code_to_query-context') }
  let!(:original_config) { capture_config }
  let!(:original_connection_config) { capture_connection_config }
  let!(:original_rake_application) { Rake.application }

  before do
    establish_test_connection
    create_test_schema
    define_test_models
    configure_context_pack
    load_context_tasks
  end

  after do
    remove_test_models
    drop_test_schema
    restore_active_record_connection
    FileUtils.remove_entry(tmpdir) if File.directory?(tmpdir)
    Rake.application = original_rake_application
    restore_config
  end

  it 'writes filtered schema metadata through code_to_query:schema' do
    invoke_task('code_to_query:schema')

    pack = read_context_pack
    users = table_named(pack, 'ctq_users')
    orders = table_named(pack, 'ctq_orders')

    expect(users).not_to be_nil
    expect(orders).not_to be_nil
    expect(column_names(users)).to include('id', 'email', 'status')
    expect_no_values(column_names(users), 'password_digest', 'reset_token')
    expect(column_names(orders)).to include('id', 'ctq_user_id', 'total_cents')
    expect_no_values(column_names(orders), 'api_key')
    expect_no_values(flattened_pack_values(pack), 'password-secret', 'reset-secret', 'api-secret')
  end

  it 'writes model metadata through code_to_query:scan_app' do
    invoke_task('code_to_query:scan_app')

    pack = read_context_pack
    models = pack.dig('models', 'models')
    associations = pack.dig('models', 'associations')
    validations = pack.dig('models', 'validations')

    expect(models.dig('CtqUser', 'table_name')).to eq('ctq_users')
    expect(models.dig('CtqOrder', 'table_name')).to eq('ctq_orders')
    expect(associations.dig('CtqUser', 'ctq_orders', 'class_name')).to eq('CtqOrder')
    expect(associations.dig('CtqOrder', 'ctq_user', 'class_name')).to eq('CtqUser')
    expect(validations.dig('CtqUser', 'email')).to be_an(Array)
    expect(pack.dig('models', 'scopes', 'CtqUser', 'active')).to include('type' => 'scope')
  end

  it 'builds and verifies a complete context pack through code_to_query:bootstrap' do
    invoke_task('code_to_query:bootstrap')

    pack = read_context_pack

    expect(pack.dig('schema', 'tables')).to be_an(Array)
    expect(pack.dig('models', 'models', 'CtqUser', 'table_name')).to eq('ctq_users')
    expect(pack['glossary']).to be_a(Hash)
    expect(pack['policies']).to be_a(Hash)
    expect(pack.dig('hints', 'joins')).to be_an(Array)
    expect_no_values(flattened_pack_values(pack), 'password_digest', 'reset_token', 'api_key')

    expect { invoke_task('code_to_query:verify') }.not_to raise_error
  end

  it 'raises from builder verification when schema tables are missing' do
    File.write(context_path, JSON.pretty_generate('models' => {}))

    expect do
      CodeToQuery::Context::Builder.new.verify!
    end.to raise_error(/missing schema\.tables/)
  end

  def establish_test_connection
    require 'active_record'

    ActiveRecord::Base.establish_connection(connection_config)
  end

  def capture_connection_config
    return unless defined?(ActiveRecord::Base) && ActiveRecord::Base.connected?

    ActiveRecord::Base.connection_db_config.configuration_hash
  rescue StandardError
    nil
  end

  def restore_active_record_connection
    return unless defined?(ActiveRecord::Base)

    ActiveRecord::Base.remove_connection
    ActiveRecord::Base.establish_connection(original_connection_config) if original_connection_config
  end

  def connection_config
    adapter = ENV.fetch('DB_ADAPTER', 'sqlite3')
    config = { adapter: adapter, database: database_name_for(adapter) }
    config[:host] = ENV['DB_HOST'] if ENV['DB_HOST']
    config[:username] = ENV['DB_USER'] if ENV['DB_USER']
    config[:password] = ENV['DB_PASSWORD'] if ENV['DB_PASSWORD']
    config
  end

  def database_name_for(adapter)
    return ENV.fetch('DB_DATABASE', ':memory:') if adapter == 'sqlite3'

    ENV.fetch('DB_DATABASE', 'code_to_query_test')
  end

  def create_test_schema
    connection = ActiveRecord::Base.connection
    drop_test_schema
    connection.create_table(:ctq_users) do |table|
      table.string :email, default: 'visible@example.com', null: false
      table.string :status, default: 'active', null: false
      table.string :password_digest, default: 'password-secret'
      table.string :reset_token, default: 'reset-secret'
      table.timestamps null: false
    end
    connection.create_table(:ctq_orders) do |table|
      table.integer :ctq_user_id, null: false
      table.integer :total_cents, null: false
      table.string :api_key, default: 'api-secret'
      table.timestamps null: false
    end
  end

  def drop_test_schema
    return unless defined?(ActiveRecord::Base)

    connection = ActiveRecord::Base.connection
    connection.drop_table(:ctq_orders, if_exists: true)
    connection.drop_table(:ctq_users, if_exists: true)
  rescue ActiveRecord::ConnectionNotEstablished, ActiveRecord::NoDatabaseError
    nil
  end

  def define_test_models
    Object.const_set(:CtqUser, Class.new(ActiveRecord::Base))
    Object.const_set(:CtqOrder, Class.new(ActiveRecord::Base))

    CtqUser.table_name = 'ctq_users'
    CtqOrder.table_name = 'ctq_orders'

    CtqUser.has_many :ctq_orders, class_name: 'CtqOrder', foreign_key: 'ctq_user_id'
    CtqUser.validates :email, presence: true
    CtqUser.scope :active, -> { where(status: 'active') }

    CtqOrder.belongs_to :ctq_user, class_name: 'CtqUser', foreign_key: 'ctq_user_id'
  end

  def remove_test_models
    Object.send(:remove_const, :CtqOrder) if Object.const_defined?(:CtqOrder)
    Object.send(:remove_const, :CtqUser) if Object.const_defined?(:CtqUser)
  end

  def configure_context_pack
    CodeToQuery.configure do |config|
      config.context_pack_path = context_path
      config.auto_glossary_with_llm = false
      config.openai_api_key = nil
      config.policy_adapter = nil
      config.prefer_static_scan = false
      config.logger = Logger.new(File::NULL)
      config.adapter = code_to_query_adapter
    end
  end

  def capture_config
    config = CodeToQuery.config
    {
      adapter: config.adapter,
      auto_glossary_with_llm: config.auto_glossary_with_llm,
      context_pack_path: config.context_pack_path,
      logger: config.logger,
      openai_api_key: config.openai_api_key,
      policy_adapter: config.policy_adapter,
      prefer_static_scan: config.prefer_static_scan
    }
  end

  def restore_config
    return unless original_config

    CodeToQuery.configure do |config|
      original_config.each do |attribute, value|
        config.public_send("#{attribute}=", value)
      end
    end
  end

  def code_to_query_adapter
    case ENV.fetch('DB_ADAPTER', 'sqlite3')
    when 'postgresql'
      :postgres
    when 'mysql2'
      :mysql
    else
      :sqlite
    end
  end

  def load_context_tasks
    Rake.application = Rake::Application.new
    Rake::Task.define_task(:environment)
    load File.expand_path('../../../tasks/code_to_query.rake', __dir__)
  end

  def invoke_task(task_name)
    Rake::Task[task_name].reenable
    Rake::Task[task_name].invoke
  end

  def read_context_pack
    JSON.parse(File.read(context_path))
  end

  def table_named(pack, table_name)
    pack.dig('schema', 'tables').find { |table| table['name'] == table_name }
  end

  def column_names(table)
    table['columns'].map { |column| column['name'] }
  end

  def flattened_pack_values(value)
    flattened = case value
                when Hash
                  value.flat_map { |key, nested_value| [key.to_s, flattened_pack_values(nested_value)] }
                when Array
                  value.flat_map { |nested_value| flattened_pack_values(nested_value) }
                else
                  value.to_s
                end

    Array(flattened).flatten
  end

  def expect_no_values(actual, *values)
    aggregate_failures do
      values.each do |value|
        expect(actual).not_to include(value)
      end
    end
  end
end
