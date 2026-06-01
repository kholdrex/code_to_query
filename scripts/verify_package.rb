# frozen_string_literal: true

require 'fileutils'
require 'open3'
require 'rbconfig'
require 'rubygems/package'
require 'tmpdir'

REPOSITORY_ROOT = File.expand_path('..', __dir__)
GEMSPEC_PATH = File.join(REPOSITORY_ROOT, 'code_to_query.gemspec')
REQUIRED_FILES = %w[
  lib/code_to_query.rb
  lib/code_to_query/version.rb
  tasks/code_to_query.rake
  README.md
  CHANGELOG.md
  LICENSE.txt
].freeze

module PackageVerification
  module_function

  def run
    Dir.mktmpdir('code_to_query-package-verify-') do |tmpdir|
      gem_path = File.join(tmpdir, 'code_to_query.gem')

      build_gem(gem_path)
      spec = inspect_gem(gem_path)
      verify_required_files!(spec, gem_path)
      verify_required_metadata!(spec)
      verify_loadable_gem!(gem_path, spec.version.to_s, tmpdir)

      puts "Verified #{File.basename(gem_path)} contains required files and loads code_to_query #{spec.version}."
    end
  end

  def build_gem(gem_path)
    run_command('gem', 'build', GEMSPEC_PATH, '--output', gem_path, chdir: REPOSITORY_ROOT)
  end

  def inspect_gem(gem_path)
    Gem::Package.new(gem_path).spec
  end

  def verify_required_files!(spec, gem_path)
    missing_from_manifest = REQUIRED_FILES - spec.files
    abort "Gem manifest is missing required files: #{missing_from_manifest.join(', ')}" unless missing_from_manifest.empty?

    package_contents = Gem::Package.new(gem_path).contents
    missing_from_package = REQUIRED_FILES - package_contents
    return if missing_from_package.empty?

    abort "Gem package is missing required files: #{missing_from_package.join(', ')}"
  end

  def verify_required_metadata!(spec)
    required_metadata = {
      'source_code_uri' => 'https://github.com/kholdrex/code_to_query',
      'rubygems_mfa_required' => 'true'
    }

    missing_metadata = required_metadata.reject do |key, expected_value|
      spec.metadata[key] == expected_value
    end
    abort "Gem package has missing or unexpected metadata: #{missing_metadata.inspect}" unless missing_metadata.empty?

    abort 'Gem package summary is missing' if spec.summary.to_s.strip.empty?
    abort 'Gem package license is missing' if spec.license.to_s.strip.empty?
    abort 'Gem package homepage is missing' if spec.homepage.to_s.strip.empty?
  end

  def verify_loadable_gem!(gem_path, version, tmpdir)
    install_dir = File.join(tmpdir, 'gem-home')
    FileUtils.mkdir_p(install_dir)

    env = isolated_gem_env(install_dir)
    run_command(
      'gem', 'install', gem_path,
      '--install-dir', install_dir,
      '--bindir', File.join(install_dir, 'bin'),
      '--no-document',
      env: env,
      chdir: tmpdir
    )

    ruby_code = <<~RUBY
      gem 'code_to_query', '#{version}'
      require 'code_to_query'
      abort 'CodeToQuery::VERSION did not load' unless defined?(CodeToQuery::VERSION)
      puts CodeToQuery::VERSION
    RUBY

    stdout = run_command(RbConfig.ruby, '-e', ruby_code, env: env, chdir: tmpdir)
    loaded_version = stdout.lines.last.to_s.strip
    return if loaded_version == version

    abort "Expected packaged gem to load version #{version}, but loaded #{loaded_version.inspect}"
  end

  def isolated_gem_env(install_dir)
    {
      'GEM_HOME' => install_dir,
      'GEM_PATH' => install_dir,
      'BUNDLE_APP_CONFIG' => nil,
      'BUNDLE_BIN_PATH' => nil,
      'BUNDLE_DEPLOYMENT' => nil,
      'BUNDLE_GEMFILE' => nil,
      'BUNDLE_PATH' => nil,
      'BUNDLE_USER_CONFIG' => nil,
      'BUNDLE_USER_HOME' => nil,
      'BUNDLE_WITH' => nil,
      'BUNDLE_WITHOUT' => nil,
      'BUNDLER_VERSION' => nil,
      'RUBYLIB' => nil,
      'RUBYOPT' => nil
    }
  end

  def run_command(*command, env: {}, chdir: nil)
    stdout, stderr, status = Open3.capture3(env, *command, chdir: chdir)
    return stdout if status.success?

    abort <<~ERROR
      Command failed (#{status.exitstatus}): #{command.join(' ')}
      STDOUT:
      #{stdout}
      STDERR:
      #{stderr}
    ERROR
  end
end

PackageVerification.run if $PROGRAM_NAME == __FILE__
