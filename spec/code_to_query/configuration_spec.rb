# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery::Configuration do
  describe '#initialize' do
    it 'defaults explain gate errors to fail closed' do
      config = described_class.send(:new)

      expect(config.explain_fail_open).to be false
    end

    it 'defaults policy adapter errors to fail closed' do
      config = described_class.send(:new)

      expect(config.policy_adapter_fail_open).to be false
    end
  end
end
