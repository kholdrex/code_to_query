# CodeToQuery

A gem that converts natural language questions into SQL queries for Rails apps. It's built for teams who need to give non-developers access to data without compromising security or performance.

## What it does

Instead of writing SQL, your team can ask questions like "Show me top customers by revenue this month" and get back safe, parameterized queries that respect your database policies and security rules. See [business question examples](docs/business-question-examples.md) for prompt patterns and review checks against a typical B2B SaaS schema.

## Key features

- **Multiple AI providers**: Works with OpenAI or local models
- **Built-in safety**: SQL linting, table allowlists, EXPLAIN plan checks
- **Schema awareness**: Understands your models, associations, and scopes
- **Policy enforcement**: Automatically injects tenant filters and access rules
- **Performance monitoring**: Optional query analysis and optimization

## Getting started

Add to your Gemfile:

```ruby
gem 'code_to_query'
```

Run `bundle install` and create a config file:

```ruby
# config/initializers/code_to_query.rb
CodeToQuery.configure do |config|
  config.openai_api_key = ENV['OPENAI_API_KEY']
  config.openai_model = 'gpt-4.1-mini'
  
  # Security settings
  config.enable_explain_gate = true
  config.allow_seq_scans = false
  config.max_query_cost = 10000
  config.require_limit_by_default = true
  config.explain_fail_open = false # default; set true only for availability-first deployments
end
```

Generate your schema context:

```bash
rails code_to_query:bootstrap
```

## Basic usage

```ruby
# Ask a question
query = CodeToQuery.ask(
  prompt: "Top 10 invoices by amount in July",
  allow_tables: %w[invoices vendors],
  current_user: current_user
)

# Check if it's safe to run
if query.safe?
  results = query.run
  puts "Found #{results.rows.length} results"

  # Shape already-materialized results for export without executing another query
  hashes = CodeToQuery::ResultExport.to_a(results)
  json = CodeToQuery::ResultExport.to_json(results)
  csv = CodeToQuery::ResultExport.to_csv(results)
end

# Or get the SQL for review
puts query.sql
puts query.params
```

## Configuration options

### Database settings
```ruby
config.adapter = :postgres           # :postgres, :mysql, :sqlite
config.readonly_role = :reporting    # Database role for queries
config.default_limit = 100           # Default row limit
config.max_limit = 10000             # Max allowed limit
```

### Security settings
```ruby
config.enable_explain_gate = true    # Block expensive/unsafe queries
config.allow_seq_scans = false       # Prevent table scans
config.max_query_cost = 10_000       # Cost threshold
config.max_joins = 3                # Join limit
config.explain_fail_open = false     # Keep EXPLAIN failures fail-closed
config.sensitive_column_patterns |= [ # Omitted from context packs
  /internal[_-]?credential/i
]
```

### Explain gate defaults and deployment profiles

When `enable_explain_gate` is on, EXPLAIN connection, adapter, and parsing errors
fail closed by default (`explain_fail_open = false`) so a degraded reporting
DB does not become a query bypass.

Choose intentionally between strict safety and availability-first behavior:

| Profile | `enable_explain_gate` | `explain_fail_open` | Recommended defaults | Use when |
| --- | --- | --- | --- | --- |
| Strict production / internal analytics | `true` | `false` | `allow_seq_scans = false`, `require_limit_by_default = true`, `max_query_cost` tuned to workload, `max_joins` set conservatively, read-only role/session | You want to default to deny unknown risk |
| Availability-first reporting | `true` | `true` | same as strict + `query_timeout`, explicit allowlists, dedicated read-only credentials, and incident alerting | You must keep low-risk reads available even when plan analysis is temporarily unavailable |
| Development / offline validation | `false` (or `true` with explicit tests) | `true` optional | low-cost limits may be intentionally reduced in local/non-production contexts | Fixture-sized tests, demos, or when EXPLAIN is unavailable locally |

If you choose availability-first behavior, treat it as an explicit risk tradeoff:

- EXPLAIN failures can let unexpectedly expensive reads proceed.
- Keep `explain_fail_open = true` behind additional operational guardrails.
- Log and monitor `code_to_query.explain_gate` failure reasons.

Read-only query sessions, conservative table allowlists, and query timeouts remain the
core guardrails regardless of profile.

Example profiles:

```ruby
# Strict internal analytics / production dashboard
config.readonly_role = :reporting
config.force_readonly_session = true
config.enable_explain_gate = true
config.explain_fail_open = false
config.require_limit_by_default = true
config.allow_seq_scans = false
config.max_query_cost = 10_000
config.max_joins = 3

# Availability-first readonly reporting database
config.readonly_role = :reporting
config.force_readonly_session = true
config.enable_explain_gate = true
config.explain_fail_open = true
config.allow_seq_scans = false
config.query_timeout = 10
config.max_query_cost = 10_000

# Developer or test environment with fixture-sized data
config.enable_explain_gate = false
config.stub_llm = true
```

### OpenAI settings
```ruby
config.openai_api_key = ENV['OPENAI_API_KEY']
config.openai_model = 'gpt-4'
config.stub_llm = false              # Set true for testing
```

## Rake tasks

```bash
rails code_to_query:bootstrap # Generate full context pack
rails code_to_query:schema    # Extract schema info
rails code_to_query:scan_app  # Scan models and associations
rails code_to_query:verify    # Check context pack integrity
```

Run the offline provider evaluation task from the gem checkout or host app bundle:

```bash
bundle exec rake code_to_query:provider:evaluate
```

Context generation omits columns whose names match `sensitive_column_patterns`
by default, including password, token, secret, credential, digest, salt, OTP,
and API key columns. Because the defaults intentionally favor omission over
exposure, remove or replace patterns in your initializer if unrelated business
columns contain those substrings. Add to the default pattern list before running
bootstrap or schema tasks if your application uses additional sensitive naming
conventions. Set `config.sensitive_column_patterns = []` if you need to generate
an unfiltered local context pack for an internal-only environment. The same filter
also removes index, foreign-key, and check-constraint metadata that references
sensitive names or definitions.

The provider evaluation task runs deterministic offline fixtures against the
planner/provider boundary and reports a pass rate for checks such as table
allowlists, bounded limits, and params represented as hashes rather than raw
strings. Treat the score as a regression signal for known prompts, not a guarantee
of production natural-language quality. Keep real provider smoke tests opt-in and
run them only with non-sensitive schema and prompts.

## Security features

- **SQL injection prevention**: All queries are parameterized
- **Access control**: Table allowlists and row-level policies
- **Performance limits**: EXPLAIN plan analysis and cost thresholds
- **Readonly execution**: Uses dedicated readonly database connections

For deployment guidance, threat boundaries, and vulnerability reporting, see [SECURITY.md](SECURITY.md).

## Advanced usage

### Custom policies
```ruby
config.policy_adapter = ->(user) do
  raise "current user is required" unless user

  predicates = { company_id: user.company_id }
  predicates[:user_id] = user.id unless user.admin?

  { enforced_predicates: predicates }
end
```

Policy adapter failures fail closed by default: adapter exceptions, `nil` returns,
and malformed predicate payloads raise `CodeToQuery::PolicyAdapterError` rather
than running an unscoped query. If you intentionally prefer availability over
row-level enforcement for a deployment, opt in explicitly:

```ruby
config.policy_adapter_fail_open = true
```

Leave `policy_adapter_fail_open` unset or `false` for the safe default.

For Rails applications that use Pundit-style policies and scopes, see the
[Pundit policy adapter examples](docs/pundit-policy-adapter.md) for conservative
table allowlists, tenant/account/user predicates, and logging boundaries.

### Upgrading EXPLAIN gate behavior

EXPLAIN gate errors fail closed by default. Existing applications that already
set `enable_explain_gate = true` and intentionally relied on the older
availability-first behavior should opt back in explicitly:

```ruby
config.explain_fail_open = true
```

Use that setting only when table allowlists, readonly credentials, row limits,
and database timeouts are strong enough for queries to proceed if EXPLAIN is
temporarily unavailable.

You can subscribe to sanitized EXPLAIN gate audit events for operational review.
The payload intentionally omits raw SQL, prompts, bind values, row data, and raw
EXPLAIN plans; it includes only decision metadata and configured thresholds.

```ruby
ActiveSupport::Notifications.subscribe('code_to_query.explain_gate') do |_name, _started, _finished, _id, payload|
  metadata = {
    adapter: payload[:adapter],
    table: payload[:table],
    query_type: payload[:query_type],
    query_shape: payload[:query_shape],
    row_limit: payload[:row_limit],
    policy_applied: payload[:policy_applied],
    allowed: payload[:allowed],
    reason: payload[:reason],
    fail_open: payload[:fail_open],
    max_query_cost: payload[:max_query_cost],
    max_query_rows: payload[:max_query_rows],
    allow_seq_scans: payload[:allow_seq_scans],
    duration_ms: payload[:duration_ms]
  }

  CodeToQuery.config.logger.info("[code_to_query] explain gate decision #{metadata.inspect}")
end
```

```ruby
ActiveSupport::Notifications.subscribe(/\Acode_to_query\./) do |_name, _started, _finished, _id, payload|
  telemetry = {
    table: payload[:table],
    query_type: payload[:query_type],
    query_shape: payload[:query_shape],
    limit: payload[:limit],
    row_limit: payload[:row_limit],
    policy_applied: payload[:policy_applied],
    bind_count: payload[:bind_count],
    filter_count: payload[:filter_count],
    join_count: payload[:join_count],
    duration_ms: payload[:duration_ms],
    failure_reason: payload[:reason],
    allowed: payload[:allowed],
    error_class: payload[:error_class]
  }

  CodeToQuery.config.logger.info("[code_to_query] pipeline telemetry #{telemetry.compact}")
end
```

### Custom schema
```ruby
schema = {
  tables: [
    {
      name: "users",
      columns: [
        { name: "id", sql_type: "integer" },
        { name: "email", sql_type: "varchar" }
      ]
    }
  ]
}

query = CodeToQuery.ask(
  prompt: "Recent users",
  schema: schema,
  allow_tables: ["users"]
)
```

## Error handling

```ruby
begin
  query = CodeToQuery.ask(prompt: "Complex query")
  results = query.run if query.safe?
rescue CodeToQuery::ExecutionError => e
  Rails.logger.error "Query failed: #{e.message}"
rescue CodeToQuery::ConnectionError => e
  Rails.logger.error "Database issue: #{e.message}"
end
```

## Contributing

1. Fork the repo
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details.
