# frozen_string_literal: true

require 'rails/railtie'

module CodeToQuery
  class Railtie < ::Rails::Railtie
    rake_tasks do
      tasks_path = File.expand_path('../../tasks/code_to_query.rake', __dir__)
      load tasks_path if File.exist?(tasks_path)
    end

    initializer 'code_to_query.configure' do
      CodeToQuery.config
    end
  end
end
