# Pundit policy adapter examples

This guide shows one way to connect CodeToQuery's `policy_adapter` to a Rails application that already uses Pundit-style policies and scopes.

CodeToQuery should sit behind your existing authorization model. Continue to use Pundit in controllers, jobs, and service objects for normal application actions. The `policy_adapter` is an additional reporting guardrail that tells CodeToQuery which tables and columns are available and which row predicates must be injected into generated queries.

## Security posture

Use a fail-closed default for reporting paths:

```ruby
# config/initializers/code_to_query.rb
CodeToQuery.configure do |config|
  config.policy_adapter_fail_open = false
  config.require_limit_by_default = true
  config.enable_explain_gate = true
  config.explain_fail_open = false
  config.force_readonly_session = true
end
```

Keep these boundaries explicit:

- **Table allowlists are required.** Pass the narrowest `allow_tables` list for each question, and have the adapter return only the Pundit-approved reporting tables for the current user.
- **Tenant predicates are mandatory.** The adapter should always return `enforced_predicates` for tenant, account, organization, or user boundaries that apply to the selected table.
- **Do not return raw SQL.** Return structured predicate values such as `{ tenant_id: current_user.tenant_id }`. CodeToQuery turns them into parameterized policy binds such as `policy_tenant_id`.
- **Pundit remains authoritative.** Use Pundit to decide whether the user can access the reporting surface. Do not use natural-language prompts as authorization input.
- **Keep logs narrow.** Prefer logging table names, policy key names, and decisions. Avoid logging raw prompts, row data, full bind values, credentials, or full generated schema context in normal application logs.

## Lambda adapter for tenant, account, and user predicates

This example keeps table and column exposure small and derives row predicates from trusted application state. It raises on missing users, unknown tables, and denied policies so CodeToQuery fails closed. Define the custom Pundit predicates such as `code_to_query?`, `view_all_tickets?`, and `view_account_users?` on the relevant policies before using this shape.

```ruby
# config/initializers/code_to_query.rb
REPORTING_MODEL_NAMES = {
  "invoices" => "Invoice",
  "payments" => "Payment",
  "tickets" => "Ticket",
  "users" => "User"
}.freeze

REPORTING_COLUMNS = {
  "invoices" => %w[id account_id tenant_id status total_cents due_on paid_at created_at],
  "payments" => %w[id account_id tenant_id invoice_id amount_cents status created_at],
  "tickets" => %w[id account_id tenant_id assigned_user_id status priority created_at],
  "users" => %w[id account_id tenant_id role created_at]
}.freeze

CodeToQuery.configure do |config|
  config.policy_adapter_fail_open = false

  config.policy_adapter = lambda do |current_user, table:, intent: nil|
    raise CodeToQuery::PolicyAdapterError, "current user is required" unless current_user

    table = table.to_s
    model = REPORTING_MODEL_NAMES.fetch(table) do
      raise CodeToQuery::PolicyAdapterError, "table is not available for reporting"
    end.constantize

    # Use Pundit to authorize the reporting surface. This does not replace the
    # normal Pundit checks used by controllers and other application code.
    policy = Pundit.policy!(current_user, model)
    unless policy.respond_to?(:code_to_query?) && policy.code_to_query?
      raise CodeToQuery::PolicyAdapterError, "reporting access denied"
    end

    # Verify that a Pundit scope class exists. The returned relation is not
    # copied into CodeToQuery; per-user enforcement still depends on the
    # structured predicates below mirroring the same access boundary.
    Pundit.policy_scope!(current_user, model)

    unless current_user.tenant_id && current_user.account_id
      raise CodeToQuery::PolicyAdapterError, "tenant and account are required"
    end

    predicates = {
      tenant_id: current_user.tenant_id,
      account_id: current_user.account_id
    }

    case table
    when "tickets"
      predicates[:assigned_user_id] = current_user.id unless policy.view_all_tickets?
    when "users"
      predicates[:id] = current_user.id unless policy.view_account_users?
    end

    {
      allowed_tables: [table],
      allowed_columns: { table => REPORTING_COLUMNS.fetch(table) },
      enforced_predicates: predicates
    }
  end
end
```

The lambda example intentionally mirrors Pundit access rules as structured predicates rather than copying SQL from `Scope#resolve`. Keep those two representations in sync with regression coverage: for each reporting role, assert that the Pundit scope and the adapter predicates expose the same tenant/account/user row set for fixture data.

Then keep each query request narrow:

```ruby
query = CodeToQuery.ask(
  prompt: "Open high-priority tickets by account",
  allow_tables: %w[tickets],
  current_user: current_user
)

preview = query.preview
Rails.logger.info(
  "code_to_query.preview " \
    "tables=tickets " \
    "applied_policy_keys=#{preview[:applied_policies].join(',')}"
)

raise "query failed safety checks" unless query.safe?
results = query.run
```

Do not log `preview[:params]` wholesale in production. `preview[:applied_policies]` contains policy bind key names such as `policy_tenant_id`, not bind values, but policy bind values can include tenant, account, and user identifiers.

## Class adapter with policy-specific predicates

A class adapter can keep Pundit integration testable and avoid large initializer lambdas.

```ruby
# app/policies/invoice_policy.rb
class InvoicePolicy < ApplicationPolicy
  def code_to_query?
    user.finance_admin? || user.billing_viewer?
  end

  def permitted_columns
    %w[id account_id tenant_id status total_cents due_on paid_at created_at]
  end

  class Scope < ApplicationPolicy::Scope
    def resolve
      return scope.none unless user

      relation = scope.where(tenant_id: user.tenant_id)
      return relation if user.finance_admin?

      relation.where(account_id: user.account_id)
    end

    # CodeToQuery reads this structured data instead of trying to copy SQL from
    # the ActiveRecord relation returned by #resolve.
    def code_to_query_predicates
      raise Pundit::NotAuthorizedError, "missing tenant" unless user&.tenant_id

      predicates = { tenant_id: user.tenant_id }
      unless user.finance_admin?
        raise Pundit::NotAuthorizedError, "missing account" unless user.account_id

        predicates[:account_id] = user.account_id
      end
      predicates
    end
  end
end
```

```ruby
# app/lib/reporting_policy_adapter.rb
class ReportingPolicyAdapter
  MODELS = {
    "invoices" => Invoice,
    "payments" => Payment
  }.freeze

  def call(current_user, table:, intent: nil)
    raise CodeToQuery::PolicyAdapterError, "current user is required" unless current_user

    table = table.to_s
    model = MODELS.fetch(table) do
      raise CodeToQuery::PolicyAdapterError, "table is not available for reporting"
    end

    policy = Pundit.policy!(current_user, model)
    unless policy.respond_to?(:code_to_query?) && policy.code_to_query?
      raise CodeToQuery::PolicyAdapterError, "reporting access denied"
    end
    unless policy.respond_to?(:permitted_columns)
      raise CodeToQuery::PolicyAdapterError, "policy does not expose reporting columns"
    end

    scope_class = Pundit::PolicyFinder.new(model).scope!
    scope = scope_class.new(current_user, model)

    unless scope.respond_to?(:code_to_query_predicates)
      raise CodeToQuery::PolicyAdapterError, "scope does not expose reporting predicates"
    end

    {
      allowed_tables: [table],
      allowed_columns: { table => Array(policy.permitted_columns).map(&:to_s) },
      enforced_predicates: scope.code_to_query_predicates
    }
  rescue Pundit::NotAuthorizedError => e
    raise CodeToQuery::PolicyAdapterError, e.message
  end
end
```

```ruby
# config/initializers/code_to_query.rb
CodeToQuery.configure do |config|
  config.policy_adapter_fail_open = false
  config.policy_adapter = ReportingPolicyAdapter.new
end
```

The important part is the separate `code_to_query_predicates` method. It mirrors the scope boundary in structured Ruby values that CodeToQuery can bind safely. Avoid extracting conditions from `scope.resolve.to_sql`, copying SQL fragments into predicates, or allowing a policy method to return user-controlled column names.

## Review checklist

Before enabling CodeToQuery for a Pundit-backed reporting endpoint, verify that:

1. The controller or service still uses normal Pundit authorization for the endpoint itself.
2. `policy_adapter_fail_open` is unset or `false` in production.
3. Every exposed table has explicit `allowed_columns` and table-specific `enforced_predicates`, with tests proving those predicates match the equivalent Pundit scope for representative users.
4. Tenant, account, organization, and user predicates are derived from trusted server-side state, not from prompts or request parameters.
5. Each `CodeToQuery.ask` call passes a request-specific `allow_tables` list.
6. Logs and telemetry omit raw prompts, row data, full params, credentials, and sensitive schema details unless they are sent to a protected audit store.
7. Read-only credentials, row limits, SQL linting, and EXPLAIN gating remain enabled for production reporting.
