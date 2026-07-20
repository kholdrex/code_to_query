# Changelog

This file tracks the major changes in each release.

## [Unreleased]

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
