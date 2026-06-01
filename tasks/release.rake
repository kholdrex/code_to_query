# frozen_string_literal: true

namespace :release do
  desc 'Build and verify the packaged gem artifact'
  task :verify_package do
    ruby File.expand_path('../scripts/verify_package.rb', __dir__)
  end
end
