# frozen_string_literal: true

RSpec.describe CodeToQuery::Context::Builder do
  describe 'sensitive schema metadata filtering' do
    let(:column_class) { Struct.new(:name, :sql_type, :type, :null, :default) }
    let(:index_class) { Struct.new(:name, :columns, :unique) }
    let(:foreign_key_class) { Struct.new(:name, :column, :to_table, :primary_key, :on_delete, :on_update) }
    let(:config) { CodeToQuery::Configuration.send(:new) }
    let(:builder) { described_class.new(config) }

    before do
      config.adapter = :sqlite
    end

    it 'omits sensitive columns from generated context packs by default' do
      connection = connection_with_columns(
        column_class.new('id', 'integer', :integer, false, nil),
        column_class.new('email', 'varchar(255)', :string, false, nil),
        column_class.new('encrypted_at', 'datetime', :datetime, true, nil),
        column_class.new('reset_count', 'integer', :integer, false, 0),
        column_class.new('password_digest', 'varchar(255)', :string, false, 'secret-hash'),
        column_class.new('reset_token', 'varchar(255)', :string, true, 'reset-secret'),
        column_class.new('token', 'varchar(255)', :string, true, 'bare-token-secret'),
        column_class.new('customer_tokenized_id', 'varchar(255)', :string, true, 'tokenized-public-id'),
        column_class.new('encrypted_api_key', 'varchar(255)', :string, true, 'key-secret')
      )

      columns = builder.send(:extract_table_columns, connection, 'users')

      expect(columns.map { |column| column[:name] }).to contain_exactly('id', 'email', 'encrypted_at', 'reset_count', 'customer_tokenized_id')
      %w[secret-hash reset-secret bare-token-secret key-secret].each do |leaked_value|
        expect(columns.to_s).not_to include(leaked_value)
      end
    end

    it 'omits bare otp columns by default without filtering arbitrary otp substrings' do
      connection = connection_with_columns(
        column_class.new('id', 'integer', :integer, false, nil),
        column_class.new('otp', 'varchar(255)', :string, true, 'bare-otp-secret'),
        column_class.new('myotpcode', 'varchar(255)', :string, true, 'non-sensitive-code'),
        column_class.new('otp_enabled', 'boolean', :boolean, false, false),
        column_class.new('recovery_otp', 'varchar(255)', :string, true, 'recovery-otp-secret')
      )

      columns = builder.send(:extract_table_columns, connection, 'users')

      expect(columns.map { |column| column[:name] }).to contain_exactly('id', 'myotpcode')
      %w[bare-otp-secret recovery-otp-secret otp_enabled recovery_otp].each do |leaked_value|
        expect(columns.to_s).not_to include(leaked_value)
      end
    end

    it 'filters index and foreign-key metadata that references sensitive columns' do
      index_connection = connection_with_indexes(
        index_class.new('index_users_on_email', ['email'], true),
        index_class.new('index_users_on_reset_token', ['reset_token'], true),
        index_class.new('index_users_on_company_and_api_key', %w[company_id api_key], false)
      )
      foreign_key_connection = connection_with_foreign_keys(
        foreign_key_class.new('fk_orders_user_id', 'user_id', 'users', 'id', nil, nil),
        foreign_key_class.new('fk_sessions_reset_token', 'reset_token', 'sessions', 'id', nil, nil)
      )

      indexes = builder.send(:extract_table_indexes, index_connection, 'users')
      foreign_keys = builder.send(:extract_foreign_keys, foreign_key_connection, 'users')

      expect(indexes).to contain_exactly(include(name: 'index_users_on_email', columns: ['email']))
      expect(foreign_keys).to contain_exactly(include(name: 'fk_orders_user_id', column: 'user_id'))
      %w[reset_token api_key].each do |leaked_name|
        expect([indexes, foreign_keys].to_s).not_to include(leaked_name)
      end
    end

    it 'filters check constraints when their names or definitions mention sensitive metadata' do
      config.adapter = :postgres
      connection = connection_with_constraints(
        { 'conname' => 'users_email_format', 'definition' => "CHECK (email LIKE '%@%')" },
        { 'conname' => 'users_password_strength', 'definition' => 'CHECK (length(password_digest) > 32)' },
        { 'conname' => 'users_description', 'definition' => "CHECK (description NOT LIKE '%secret%')" }
      )

      constraints = builder.send(:extract_table_constraints, connection, 'users')

      expect(constraints).to contain_exactly(include(name: 'users_email_format', type: 'check'))
      %w[password_digest secret users_password_strength].each do |leaked_value|
        expect(constraints.to_s).not_to include(leaked_value)
      end
    end

    it 'allows applications to override sensitive column patterns with strings or regexps' do
      config.sensitive_column_patterns = ['internal_code', /password/i]
      connection = connection_with_columns(
        column_class.new('id', 'integer', :integer, false, nil),
        column_class.new('password_digest', 'varchar(255)', :string, false, nil),
        column_class.new('customer_internal_code', 'varchar(255)', :string, true, nil),
        column_class.new('reset_token', 'varchar(255)', :string, true, nil)
      )

      columns = builder.send(:extract_table_columns, connection, 'users')

      expect(columns.map { |column| column[:name] }).to contain_exactly('id', 'reset_token')
      %w[password_digest customer_internal_code].each do |leaked_name|
        expect(columns.to_s).not_to include(leaked_name)
      end
    end

    def connection_with_columns(*column_list)
      Class.new do
        define_method(:primary_key) { |_table_name| 'id' }
        define_method(:columns) { |_table_name| column_list }
      end.new
    end

    def connection_with_indexes(*index_list)
      Class.new do
        define_method(:indexes) { |_table_name| index_list }
      end.new
    end

    def connection_with_constraints(*constraint_rows)
      Class.new do
        define_method(:execute) { |_sql| constraint_rows }
      end.new
    end

    def connection_with_foreign_keys(*foreign_key_list)
      Class.new do
        define_method(:foreign_keys) { |_table_name| foreign_key_list }
      end.new
    end
  end
end
