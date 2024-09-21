# frozen_string_literal: true

namespace :code_to_query do
  desc 'Bootstrap complete context pack (schema + app scan + glossary + policies)'
  task bootstrap: :environment do
    require 'code_to_query'

    puts 'Starting CodeToQuery bootstrap process...'

    begin
      pack = CodeToQuery::Context::Builder.bootstrap!

      puts 'Context pack created.'
      puts "Location: #{CodeToQuery.config.context_pack_path}"
      puts "Tables: #{pack.schema[:tables]&.length || 0}"
      puts "Models: #{pack.models[:models]&.length || 0}"
      puts "Glossary terms: #{pack.glossary&.length || 0}"

      # Run verification
      Rake::Task['code_to_query:verify'].invoke
    rescue StandardError => e
      puts "Bootstrap failed: #{e.message}"
      puts 'Try running individual tasks to debug:'
      puts '   rake code_to_query:schema'
      puts '   rake code_to_query:scan_app'
      puts '   rake code_to_query:glossary'
      puts '   rake code_to_query:policies'
      exit 1
    end
  end

  desc 'Extract schema information from database'
  task schema: :environment do
    require 'code_to_query'

    puts 'Extracting database schema...'

    begin
      builder = CodeToQuery::Context::Builder.new
      schema = builder.extract_schema

      pack_data = load_existing_pack
      pack_data['schema'] = schema
      pack_data['updated_at'] = Time.now.iso8601

      write_pack_data(pack_data)

      puts 'Schema extracted.'
      puts "Tables: #{schema[:tables]&.length || 0}"
      puts "Adapter: #{schema[:adapter]}"
      puts "Version: #{schema[:version]}"
    rescue StandardError => e
      puts "Schema extraction failed: #{e.message}"
      exit 1
    end
  end

  desc 'Scan Rails application models and associations'
  task scan_app: :environment do
    require 'code_to_query'

    puts 'Scanning Rails application...'

    begin
      builder = CodeToQuery::Context::Builder.new
      app_data = builder.scan_app

      pack_data = load_existing_pack
      pack_data['models'] = app_data
      pack_data['updated_at'] = Time.now.iso8601

      write_pack_data(pack_data)

      puts 'Application scan completed.'
      puts "Models: #{app_data[:models]&.length || 0}"
      puts "Associations: #{app_data[:associations]&.length || 0}"
    rescue StandardError => e
      puts "Application scan failed: #{e.message}"
      exit 1
    end
  end

  desc 'Generate business glossary and synonyms'
  task glossary: :environment do
    require 'code_to_query'

    puts 'Generating business glossary...'

    begin
      builder = CodeToQuery::Context::Builder.new
      glossary = builder.generate_glossary

      pack_data = load_existing_pack
      pack_data['glossary'] = glossary
      pack_data['updated_at'] = Time.now.iso8601

      write_pack_data(pack_data)

      puts 'Glossary generated.'
      puts "Terms: #{glossary&.length || 0}"
    rescue StandardError => e
      puts "Glossary generation failed: #{e.message}"
      exit 1
    end
  end

  desc 'Collect security policies and access rules'
  task policies: :environment do
    require 'code_to_query'

    puts 'Collecting security policies...'

    begin
      builder = CodeToQuery::Context::Builder.new
      policies = builder.collect_policies

      pack_data = load_existing_pack
      pack_data['policies'] = policies
      pack_data['updated_at'] = Time.current.iso8601

      write_pack_data(pack_data)

      puts 'Policies collected.'
    rescue StandardError => e
      puts "Policy collection failed: #{e.message}"
      exit 1
    end
  end

  desc 'Verify context pack integrity and completeness'
  task verify: :environment do
    require 'code_to_query'

    puts 'Verifying context pack...'

    begin
      builder = CodeToQuery::Context::Builder.new
      builder.verify!

      # Additional comprehensive verification
      pack_data = load_existing_pack

      # Check schema completeness
      schema = pack_data['schema'] || {}
      tables = schema['tables'] || []

      puts 'Context pack verification passed.'
      puts "Schema: #{tables.length} tables"

      if pack_data['models']
        models = pack_data['models']['models'] || {}
        puts "Models: #{models.length} models"
      end

      puts "Glossary: #{pack_data['glossary'].length} terms" if pack_data['glossary']

      # Check for potential issues
      warnings = []
      warnings << 'No tables found in schema' if tables.empty?
      warnings << 'No models found' if pack_data.dig('models', 'models') && pack_data.dig('models', 'models').empty?
      warnings << 'No glossary terms' if pack_data['glossary'] && pack_data['glossary'].empty?

      if warnings.any?
        puts "\nWarnings:"
        warnings.each { |warning| puts "   - #{warning}" }
      end

      puts "\nContext pack location: #{CodeToQuery.config.context_pack_path}"
    rescue StandardError => e
      puts "Verification failed: #{e.message}"
      exit 1
    end
  end

  desc 'Show context pack statistics and information'
  task info: :environment do
    require 'code_to_query'

    puts 'CodeToQuery Context Pack Information'
    puts '=' * 50

    begin
      pack_data = load_existing_pack

      puts "Location: #{CodeToQuery.config.context_pack_path}"
      puts "Last updated: #{pack_data['updated_at'] || 'Unknown'}"
      puts

      # Schema info
      if pack_data['schema']
        schema = pack_data['schema']
        puts 'Database Schema:'
        puts "   Adapter: #{schema['adapter']}"
        puts "   Version: #{schema['version']}"
        puts "   Tables: #{schema['tables']&.length || 0}"

        if schema['tables']&.any?
          puts '   Sample tables:'
          schema['tables'].first(5).each do |table|
            column_count = table['columns']&.length || 0
            puts "     - #{table['name']} (#{column_count} columns)"
          end
          puts '     ...' if schema['tables'].length > 5
        end
        puts
      end

      # Models info
      if pack_data['models']
        models_data = pack_data['models']
        models = models_data['models'] || {}
        associations = models_data['associations'] || {}

        puts 'Rails Models:'
        puts "   Models: #{models.length}"
        puts "   With associations: #{associations.length}"

        if models.any?
          puts '   Sample models:'
          models.first(5).each do |name, info|
            puts "     - #{name} → #{info['table_name']}"
          end
          puts '     ...' if models.length > 5
        end
        puts
      end

      # Glossary info
      if pack_data['glossary']
        glossary = pack_data['glossary']
        puts 'Business Glossary:'
        puts "   Terms: #{glossary.length}"

        if glossary.any?
          puts '   Sample terms:'
          glossary.first(5).each do |term, synonyms|
            puts "     - #{term}: #{Array(synonyms).join(', ')}"
          end
          puts '     ...' if glossary.length > 5
        end
        puts
      end

      # Configuration info
      config = CodeToQuery.config
      puts 'Configuration:'
      puts "   Database adapter: #{config.adapter}"
      puts "   Default limit: #{config.default_limit}"
      puts "   Max limit: #{config.max_limit}"
      puts "   Readonly role: #{config.readonly_role || 'None'}"
      puts "   OpenAI model: #{config.openai_model}"
      puts "   Explain gate: #{config.enable_explain_gate ? 'Enabled' : 'Disabled'}"
    rescue StandardError => e
      puts "Could not load context pack: #{e.message}"
      puts "   Run 'rake code_to_query:bootstrap' to create it."
    end
  end

  desc 'Clean and rebuild context pack'
  task rebuild: :environment do
    require 'code_to_query'

    puts 'Rebuilding context pack...'

    # Remove existing pack
    if File.exist?(CodeToQuery.config.context_pack_path)
      File.delete(CodeToQuery.config.context_pack_path)
      puts 'Removed existing context pack'
    end

    # Run bootstrap
    Rake::Task['code_to_query:bootstrap'].invoke
  end

  desc 'Test query generation with sample prompts'
  task test: :environment do
    require 'code_to_query'

    puts 'Testing CodeToQuery with sample prompts...'

    sample_prompts = [
      'Show me all users created this month',
      'Top 10 orders by amount',
      'Find invoices from July 2023',
      'Count active customers',
      'Recent payments over $1000'
    ]

    sample_prompts.each_with_index do |prompt, index|
      puts "\n#{index + 1}. Testing: \"#{prompt}\""

      begin
        query = CodeToQuery.ask(prompt: prompt)
        puts "   SQL: #{query.sql}"
        puts "   Params: #{query.params.inspect}"
        puts "   Safe: #{query.safe?}"
      rescue StandardError => e
        puts "   Error: #{e.message}"
      end
    end

    puts "\nTest completed."
  end

  private

  def load_existing_pack
    path = CodeToQuery.config.context_pack_path
    if File.exist?(path)
      JSON.parse(File.read(path))
    else
      {}
    end
  rescue JSON::ParserError
    puts 'Existing context pack has invalid JSON, starting fresh'
    {}
  end

  def write_pack_data(data)
    path = CodeToQuery.config.context_pack_path
    dir = File.dirname(path)
    FileUtils.mkdir_p(dir)

    File.write(path, JSON.pretty_generate(data))
  end
end
