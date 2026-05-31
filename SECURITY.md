# Security Policy

CodeToQuery is designed as a safety-first query generation library for Rails applications. It can reduce the risk of ad hoc reporting, but it is not a substitute for normal database, authorization, monitoring, and incident-response controls.

## Supported versions

Security fixes are currently applied to the latest released gem line and the `master` branch. Until CodeToQuery reaches a stable 1.0 release, older minor versions are not backported; applications should track the latest release and review the changelog before upgrading.

## Security model

CodeToQuery treats the language model, user prompt, generated intent, schema context, and generated SQL as untrusted until they pass the configured guardrails. A safe deployment should layer multiple independent controls:

- read-only database credentials or read-only sessions;
- explicit table allowlists;
- row-level policy predicates from application authorization state;
- parameterized SQL for user-provided values;
- SQL linting in `CodeToQuery::Guardrails::SqlLinter` that rejects writes, stacked statements, SQL comments, dangerous functions, adapter-specific system metadata access, and unparameterized string or multi-digit numeric WHERE literals;
- required row limits and maximum limit enforcement;
- optional EXPLAIN-based cost and scan checks;
- audit instrumentation with non-sensitive payloads by default.

The default posture should be fail closed. If a guardrail cannot prove that a query is safe, the query should not run unless the application has deliberately opted into an availability-first mode and has compensating controls.

## Threat model

### Prompt injection and intent manipulation

Users may ask for unsafe actions, try to override policy rules, or phrase requests as instructions to bypass guardrails. Treat prompts as user input, not authority. Application policy, table allowlists, parameter binding, SQL linting, and EXPLAIN checks must remain authoritative even when the prompt asks for broader access.

### Cross-tenant data exposure

The highest-impact failure is dropping or weakening tenant, account, organization, or user predicates. Policy adapter errors, malformed policy results, and conflicting prompt-derived filters fail closed by default. Keep `policy_adapter_fail_open` unset or `false` for production and internal analytics. Only consider `policy_adapter_fail_open = true` after verifying that database-side row-level security, per-tenant roles, or equivalent controls independently enforce the tenant boundary; even then, prefer to keep the fail-closed default.

### Expensive or disruptive reads

Read-only queries can still cause outages by scanning large tables, joining too broadly, or returning excessive rows. Keep row limits, join limits, query timeouts, read-only roles, and the EXPLAIN gate enabled for production reporting. Run EXPLAIN through the same read-only role or connection path that will execute the query so plan checks do not hide permission errors. `explain_fail_open = true` should be reserved for deliberately availability-first deployments where the database can tolerate otherwise safe reads during temporary EXPLAIN failures.

### Sensitive schema leakage

Context packs can reveal sensitive application structure even when row data is never included. CodeToQuery filters common sensitive column names and related index, foreign-key, and check-constraint metadata by default. Review generated context before sharing it outside the application boundary, and extend `sensitive_column_patterns` for application-specific names.

### LLM provider exposure

Prompts, schema context, glossary data, and few-shot examples may be sent to the configured LLM provider. Review provider retention, training, logging, residency, and network-egress terms before using third-party models with production schema context. Prefer a local provider or a redacted context pack for regulated environments, and never include row data, credentials, secrets, or raw customer content in provider prompts.

### SQL injection and generated SQL drift

Generated SQL must be treated as untrusted output until it is parameterized, validated, linted, and bounded. Do not concatenate prompt text, model output, or user values into SQL. When adding compiler features, include regression tests for parameter binding, adapter quoting, table allowlists, policy predicates, row limits, and lint rejection behavior.

### Observability data exposure

Logs, metrics, notifications, and audit events can become a secondary data leak. Instrumentation payloads should avoid raw prompts, generated answers, row data, bind values that may contain user content, API keys, credentials, and full schema dumps unless the host application explicitly chooses to record them in a protected audit store. Review subscribers and logger configuration before exporting events to third-party telemetry systems.

## Deployment guidance

For production or internal business analytics, prefer this baseline:

```ruby
CodeToQuery.configure do |config|
  config.readonly_role = :reporting
  config.force_readonly_session = true
  config.require_limit_by_default = true
  config.enable_explain_gate = true
  config.explain_fail_open = false
  config.policy_adapter_fail_open = false
  config.allow_seq_scans = false
  config.max_joins = 3
  config.max_query_cost = 10_000
end
```

`max_joins` and `max_query_cost` are starting points, not universal limits; tune them against representative EXPLAIN plan costs for your application and adapter. Use a dedicated reporting database role whenever possible. `force_readonly_session` is a useful Postgres/MySQL backstop, but it should not be treated as a replacement for least-privilege database grants. Keep application authorization and database permissions narrower than the natural-language interface. For high-risk environments, also enforce native database row-level security or reporting replicas with limited grants.

## Vulnerability reporting

Please report suspected vulnerabilities privately to <alexandrkholodniak@gmail.com> rather than opening a public issue with exploit details. If GitHub private vulnerability reporting is enabled for the repository, you may also use the repository's **Security** tab. Include:

- affected CodeToQuery version or commit;
- Rails, Ruby, database adapter, and database versions;
- the relevant configuration flags;
- a minimal prompt, intent, or SQL example if it can be shared safely;
- whether sensitive data exposure, policy bypass, injection, or denial of service is suspected.

The project owner will triage the report, coordinate a fix when appropriate, and publish user-facing upgrade guidance in the changelog.
