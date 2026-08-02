# frozen_string_literal: true

require 'spec_helper'

RSpec.describe CodeToQuery::PolicyAdapterInvoker do
  let(:current_user) { { id: 7 } }
  let(:intent) { { 'table' => 'orders' } }

  def invoke(adapter)
    described_class.call(adapter, current_user, table: 'orders', intent: intent)
  end

  def adapter_forms(implementation)
    proc_adapter = implementation

    method_owner = Object.new
    method_owner.define_singleton_method(:call, &implementation)

    callable_class = Class.new do
      define_method(:call, &implementation)
    end

    {
      proc: proc_adapter,
      method: method_owner.method(:call),
      callable_object: callable_class.new
    }
  end

  it 'passes table and intent to explicit keyword adapters in every callable form' do
    implementation = proc { |user, table:, intent:| [user, table, intent] }
    adapter_forms(implementation).each do |form, candidate|
      expect(invoke(candidate)).to eq([current_user, 'orders', intent]), form.to_s
    end
  end

  it 'passes the complete context to keyrest adapters in every callable form' do
    implementation = proc { |user, **context| [user, context] }
    adapter_forms(implementation).each do |form, candidate|
      expect(invoke(candidate)).to eq([current_user, { table: 'orders', intent: intent }]), form.to_s
    end
  end

  it 'passes context as a positional hash to rest adapters in every callable form' do
    implementation = proc { |*args| args }
    adapter_forms(implementation).each do |form, candidate|
      expect(invoke(candidate)).to eq([current_user, { table: 'orders', intent: intent }]), form.to_s
    end
  end

  it 'passes context as a positional hash to legacy optional-options adapters in every callable form' do
    implementation = proc { |user, options = {}| [user, options] }
    adapter_forms(implementation).each do |form, candidate|
      expect(invoke(candidate)).to eq([current_user, { table: 'orders', intent: intent }]), form.to_s
    end
  end

  it 'selects only table for table-only keyword adapters in every callable form' do
    implementation = proc { |user, table:| [user, table] }
    adapter_forms(implementation).each do |form, candidate|
      expect(invoke(candidate)).to eq([current_user, 'orders']), form.to_s
    end
  end

  it 'passes only current user to user-only adapters in every callable form' do
    implementation = proc { |user| user }
    adapter_forms(implementation).each do |form, candidate|
      expect(invoke(candidate)).to eq(current_user), form.to_s
    end
  end

  it 'invokes each adapter only once when it raises a signature-like ArgumentError internally' do
    calls = Hash.new(0)
    current_form = nil

    implementation = proc do |_user, table:, intent:|
      calls[current_form] += 1
      raise ArgumentError, 'wrong number of arguments' if table == 'orders' && intent
    end

    adapter_forms(implementation).each do |form, candidate|
      current_form = form

      expect { invoke(candidate) }.to raise_error(ArgumentError, 'wrong number of arguments')
      expect(calls[form]).to eq(1)
    end
  end
end
