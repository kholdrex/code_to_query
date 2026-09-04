# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery::Instrumentation do
  describe '.instrument' do
    let(:events) { [] }
    let(:subscriber) do
      ActiveSupport::Notifications.subscribe(/\Acode_to_query\./) do |name, _started, _finished, _id, payload|
        events << [name, payload]
      end
    end

    before { subscriber }

    after do
      ActiveSupport::Notifications.unsubscribe(subscriber) if subscriber
    end

    it 'emits the stage name, safe decision metadata, and duration on success' do
      result = described_class.instrument(
        :validate,
        query_shape: 'select:users',
        allowed: true,
        reason: :safe_plan,
        duration_ms: 'caller value',
        sql: 'SELECT * FROM users WHERE email = leak@example.test',
        binds: ['secret-bind'],
        prompt: 'Get users',
        schema: { users: ['email'] }
      ) { :result }

      expect(result).to eq(:result)
      expect(events).to contain_exactly(
        ['code_to_query.validate', include(query_shape: 'select:users', allowed: true, reason: :safe_plan,
                                           duration_ms: a_kind_of(Numeric))]
      )
      payload = events.first.last
      expect(payload.keys).to match_array(%i[query_shape allowed reason duration_ms])
      expect(payload.inspect).not_to include('SELECT * FROM users', 'secret-bind', 'Get users', 'email')
    end

    it 'emits only sanitized metadata and exception class on failure, then re-raises' do
      error = RuntimeError.new('failed while executing SELECT * FROM users with secret-bind')

      expect do
        described_class.instrument(
          :compile,
          query_shape: 'select:users',
          policy_applied: true,
          sql: 'SELECT * FROM users',
          params: { email: 'secret-bind' },
          prompt: 'Get users',
          schema: { users: ['email'] }
        ) { raise error }
      end.to raise_error(error)

      expect(events).to contain_exactly(
        ['code_to_query.compile', include(query_shape: 'select:users', policy_applied: true,
                                          error_class: 'RuntimeError', duration_ms: a_kind_of(Numeric))]
      )
      payload = events.first.last
      expect(payload.keys).to match_array(%i[query_shape policy_applied error_class duration_ms])
      expect(payload.inspect).not_to include('SELECT * FROM users', 'secret-bind', 'Get users', 'email', error.message)
    end
  end
end
