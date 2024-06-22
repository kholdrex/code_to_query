# frozen_string_literal: true

require_relative 'lib/code_to_query/version'

Gem::Specification.new do |spec|
  spec.name          = 'code_to_query'
  spec.version       = CodeToQuery::VERSION
  spec.authors       = ['Alex Kholodniak']
  spec.email         = ['alexandrkholodniak@gmail.com']

  spec.summary       = 'Natural language to safe, parameterized SQL for Rails'
  spec.description   = 'Ask for data in plain English; get validated, parameterized SQL with guardrails.'
  spec.homepage      = 'https://github.com/CodeToQuery/code_to_query'
  spec.license       = 'MIT'

  spec.required_ruby_version = '>= 3.0'

  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    Dir['lib/**/*', 'tasks/**/*', 'README*', 'LICENSE*', 'CHANGELOG*']
  end
  spec.require_paths = ['lib']

  spec.metadata = {
    'source_code_uri' => 'https://github.com/CodeToQuery/code_to_query',
    'rubygems_mfa_required' => 'true'
  }

  # Depend on ActiveRecord/ActiveSupport directly to allow matrix testing
  # across multiple AR versions without pulling full Rails and locking AR.
  spec.add_dependency 'activerecord', '>= 6.1'
  spec.add_dependency 'activesupport', '>= 6.1'
  spec.add_dependency 'dry-schema', '>= 1.13'
  spec.add_dependency 'dry-types', '>= 1.7'
  spec.add_dependency 'mutex_m' # Required for Ruby 3.4+ compatibility

  # Optional dependencies for enhanced functionality
  spec.add_development_dependency 'bundler'
  spec.add_development_dependency 'pg', '>= 1.1'
  spec.add_development_dependency 'rake'
  spec.add_development_dependency 'rspec', '~> 3.0'
  spec.add_development_dependency 'rspec-rails'
  spec.add_development_dependency 'sqlite3', '~> 1.4'
end
