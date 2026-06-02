# Business question examples

These examples show the kinds of internal reporting questions CodeToQuery is designed to handle for a typical multi-tenant Rails application. They are prompts and review expectations, not guarantees that every application schema will compile to the same SQL.

The examples assume a canonical B2B SaaS schema with tables such as `accounts`, `users`, `subscriptions`, `invoices`, `payments`, `refunds`, `events`, and `tickets`. In production, keep `allow_tables` scoped to the smallest set needed for the question and rely on policy predicates for tenant or user access rules.

## Revenue and billing

| Question | Suggested allowlist | What to review before running |
| --- | --- | --- |
| Top 10 accounts by paid invoice total this month | `accounts`, `invoices`, `payments` | Date range, paid-status filter, tenant policy bind, and row limit |
| Monthly recurring revenue by plan for the last 6 months | `subscriptions`, `accounts` | Active subscription filter, grouping columns, and whether archived accounts are excluded |
| Invoices overdue by more than 30 days | `invoices`, `accounts` | Overdue cutoff date, unpaid-status filter, and selected account columns |
| Refund total by reason this quarter | `payments`, `refunds` | Refund-only filter, reason grouping, and absence of customer PII in selected columns |

## Product usage

| Question | Suggested allowlist | What to review before running |
| --- | --- | --- |
| Daily active users for the last 14 days | `users`, `events` | Event-name filter, time window, distinct-user aggregation, and limit |
| Accounts with no activity in the last 30 days | `accounts`, `events` | Anti-join or subquery shape, activity timestamp filter, and tenant policy bind |
| Most-used features by plan this month | `events`, `subscriptions`, `accounts` | Feature event allowlist, plan grouping, and bounded result set |
| Users who triggered export more than five times this week | `users`, `events` | Export event filter, count threshold, and selected user fields |

## Customer operations

| Question | Suggested allowlist | What to review before running |
| --- | --- | --- |
| Open high-priority tickets by account | `tickets`, `accounts` | Open-status and priority filters, grouping, and account visibility policy |
| Accounts with renewal in the next 45 days and open tickets | `accounts`, `subscriptions`, `tickets` | Renewal date window, open-ticket join, and duplicate account handling |
| Average first response time by support tier this month | `tickets`, `accounts` | Timestamp arithmetic support for the current adapter and grouping by tier |
| Customers with failed payment and no ticket in the last 7 days | `accounts`, `payments`, `tickets` | Failed-payment filter, ticket absence condition, and safe selected columns |

## Safe review checklist

Before exposing a generated query to non-developers or running it against production data, check that:

1. The table allowlist matches the business question and excludes unrelated sensitive tables.
2. Tenant or access-policy predicates that use policy-prefixed bind keys are visible in `preview[:applied_policies]`. Also review the SQL shape for policy predicates that do not require bind values.
3. The query is read-only, parameterized, and includes an explicit or default limit unless it is an aggregate-only result.
4. Selected columns avoid secrets, tokens, raw message bodies, and unnecessary personal data.
5. EXPLAIN gating is enabled for production paths and `config.explain_fail_open` remains `false` unless you have deliberately configured it to fail open for availability reasons.
6. Audit subscribers record the query shape and decision metadata, not raw prompts, row data, or sensitive bind values.

## Preview before execution

Use `Query#preview` to review SQL, bind values, and policy application before calling `run`. The preview hash exposes `:sql`, `:params`, `:applied_policies`, `:estimated_cost`, and `:would_run?`. `preview[:would_run?]` reports the preview lint result; it is not a substitute for `query.safe?`, which also applies configured safety checks such as EXPLAIN gating and policy compliance.

```ruby
allow_tables = %w[accounts invoices payments]
query = CodeToQuery.ask(
  prompt: "Top 10 accounts by paid invoice total this month",
  allow_tables: allow_tables,
  current_user: current_user
)

preview = query.preview
raise "query rejected by preview lint" unless preview[:would_run?]
raise "query failed safety checks" unless query.safe?

Rails.logger.info(
  "code_to_query.preview " \
    "tables=#{allow_tables.join(',')} " \
    "applied_policy_keys=#{preview[:applied_policies].join(',')} " \
    "safety_checked=true"
)
```

Do not log `preview[:params]` wholesale in production. Parameter values can contain tenant identifiers or other sensitive values even when the SQL shape is safe. `preview[:applied_policies]` contains policy bind key names only; prefer logging those key names over bind values when auditing policy application.
