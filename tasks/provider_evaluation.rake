# frozen_string_literal: true

namespace :code_to_query do
  namespace :provider do
    desc 'Run deterministic offline provider evaluation fixtures'
    task :evaluate, [:fixture_path] do |_task, args|
      require 'code_to_query'
      require 'code_to_query/provider_evaluation'

      fixture_path = args[:fixture_path] || CodeToQuery::ProviderEvaluation::DEFAULT_FIXTURE_PATH
      evaluation = CodeToQuery::ProviderEvaluation.load_file(fixture_path).run
      summary = evaluation.summary

      puts "Provider evaluation: #{summary[:passed]}/#{summary[:total]} passed (#{format('%.1f', summary[:success_rate] * 100)}%)"

      evaluation.results.reject(&:passed).each do |result|
        puts "- #{result.case_name}: #{result.failures.join('; ')}"
      end

      abort 'Provider evaluation failed' unless evaluation.passed?
    end
  end
end
