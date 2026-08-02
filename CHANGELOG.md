# Changelog

This file tracks the major changes in each release.

## [0.2.0] - 2026-08-02

### Upgrading from 0.1.0

This release flips several defaults from fail-open to fail-closed. Applications that relied on the previous permissive behavior will start raising instead of silently degrading.

- EXPLAIN gate errors now block the query. If your deployment must stay available when the adapter cannot run EXPLAIN, set `config.explain_fail_open = true`.
- Policy adapter failures now block the query instead of dropping the enforced predicate. Set `config.policy_adapter_fail_open = true` only when availability outranks row-level enforcement.
- Build executable queries through `CodeToQuery.ask` when a `policy_adapter` is configured. Directly constructed `Query` objects carry no policy contract and are rejected.
- Recompile queries after swapping the policy adapter or the configuration it depends on. Query objects cached across such a change are invalidated by design.
- Policy adapters returning `allowed_tables: []` now deny every table. Omit the key entirely when you do not intend to supply a policy table allowlist.
- `Query#sql`, `#params`, `#intent`, and `#metrics` return detached copies. Code that mutated those return values to alter query state must use the supported APIs instead.
- Context packs now omit sensitive column metadata. Regenerate them with `rake code_to_query:rebuild`, and extend `config.sensitive_column_patterns` if your schema uses other naming conventions.

### Added
- Added sanitized EXPLAIN gate audit instrumentation for safe ActiveSupport subscriber logging of decision metadata and thresholds. ExplainGate error logs now record exception classes only so adapter error messages cannot leak SQL or bind fragments.
- Added result export helpers for shaping already-returned database results into array-of-hashes, JSON, and CSV strings.
- Added Pundit policy adapter examples for fail-closed Rails reporting integrations with table allowlists and tenant predicates.
- Added CI package verification for the built gem artifact, including packaged files, metadata, and loadability from an isolated install path.
- Added deterministic offline provider evaluation fixtures and a `code_to_query:provider:evaluate` task for tracking planner/provider safety regressions.
- Added a security policy and threat model covering safe deployment defaults, cross-tenant data exposure, schema-context filtering, observability data exposure, and vulnerability reporting.

### Changed
- Security compatibility: when a `policy_adapter` is configured, executable `Query` objects must come from `CodeToQuery.ask` (or the compiler path) with the compiler's opaque policy contract. Directly constructed queries have no valid contract and now fail closed.
- `Query#sql`, `#params`, `#intent`, and `#metrics` now return detached deep copies. Mutating a returned value no longer changes query state; repeated reads may allocate new objects.
- Compiled policy queries are bound to the exact policy adapter and configuration identity used at compilation. Replacing the adapter or relevant configuration invalidates existing queries; compile them again before execution.
- Compatibility note: policy adapters that explicitly return `allowed_tables: []` deny access to all tables, as intended; omit `allowed_tables` only when no policy table allowlist is being supplied.
- Added explicit explain-gate profiles and deployment tradeoff guidance for `explain_fail_open` in the README, including strict, availability-first, and local/test configuration patterns.
- Breaking default change: EXPLAIN gate errors now fail closed by default; set `explain_fail_open = true` only for availability-first readonly deployments that can tolerate skipped EXPLAIN checks.
- Context pack generation now omits sensitive schema metadata by default for password, secret, token, credential, digest, salt, OTP, and API key style columns; customize `sensitive_column_patterns` when applications use additional naming conventions.
- Policy adapter failures now fail closed by default instead of silently omitting enforced predicates; set `policy_adapter_fail_open = true` only when availability must take precedence over row-level policy enforcement.

## [0.1.0] - 2025-08-14

### Added
- Support for OpenAI and local AI providers
- SQL safety checks with table allowlists and EXPLAIN analysis
- Automatic policy enforcement for row-level security
- Query performance monitoring and caching
- CI setup for multiple Ruby and Rails versions
- Basic gem structure and setup
- Core Query class with SQL generation and safety checks
- Simple query validation using dry-schema
- Local query planning and compilation
- MIT license and initial docs

### Changed
- Simplified the SQL linter to work better with different database adapters
- Improved the query compiler with better Arel integration
- Cleaner error messages and logging
