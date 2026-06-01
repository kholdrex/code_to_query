# frozen_string_literal: true

require 'rails/railtie'

module CodeToQuery
  class Railtie < ::Rails::Railtie
    rake_tasks do
      %w[code_to_query provider_evaluation].each do |task_name|
        tasks_path = File.expand_path("../../tasks/#{task_name}.rake", __dir__)
        load tasks_path if File.exist?(tasks_path)
      end
    end

    initializer 'code_to_query.configure' do
      CodeToQuery.config
    end
  end
end
