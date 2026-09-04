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

    SAFE_PAYLOAD_KEYS = %i[
      adapter allow_seq_scans allowed bind_count error_class fail_open filter_count join_count
      limit max_query_cost max_query_rows policy_applied query_shape query_type reason
      row_limit table
    ].freeze

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
    rescue StandardError, SecurityError => e
      # Instrumentation is intentionally non-blocking. Subscriber failures should never
      # change behavior, so log and continue.
      CodeToQuery.config.logger.warn("[code_to_query] Telemetry publish failed: #{e.class.name}")
      nil
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def telemetry_payload(payload, started:)
      sanitized_payload(payload).merge(duration_ms: elapsed_ms(started))
    end

    def sanitized_payload(payload)
      payload.each_with_object({}) do |(key, value), sanitized|
        normalized_key = key.to_sym
        sanitized[normalized_key] = value if SAFE_PAYLOAD_KEYS.include?(normalized_key)
      end
    end

    def elapsed_ms(started_at)
      ((monotonic_time - started_at) * 1_000).round(3)
    end
  end
end
