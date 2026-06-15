# frozen_string_literal: true

begin
  require 'active_support'
  require 'active_support/notifications'
rescue LoadError
end

require 'securerandom'

module CodeToQuery
  module Instrumentation
    module_function

    def instrument(stage, payload = {})
      event_name = "code_to_query.#{stage}"
      safe_payload = payload
      started = monotonic_time
      return publish(event_name, telemetry_payload(safe_payload, started: started), started: started) unless block_given?

      result = yield
      publish(event_name, telemetry_payload(safe_payload, started: started), started: started)
      result
    # SecurityError does not inherit from StandardError; keep it explicit so
    # rejected SQL paths still emit sanitized failure telemetry.
    rescue StandardError, SecurityError => e
      publish(
        event_name,
        telemetry_payload(safe_payload.merge(error_class: e.class.name), started: started),
        started: started
      )
      raise
    end

    def notifications_available?
      defined?(ActiveSupport::Notifications) && ActiveSupport::Notifications.respond_to?(:publish)
    end

    def publish(event_name, payload, started: monotonic_time)
      return unless notifications_available?

      ActiveSupport::Notifications.publish(event_name, started, monotonic_time, SecureRandom.uuid, payload)
      nil
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def telemetry_payload(payload, started:)
      payload.merge(duration_ms: elapsed_ms(started))
    end

    def elapsed_ms(started_at)
      ((monotonic_time - started_at) * 1_000).round(3)
    end
  end
end
