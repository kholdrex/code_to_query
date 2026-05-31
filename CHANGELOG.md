# Changelog

This file tracks the major changes in each release.

## [Unreleased]

### Added
- Added deterministic offline provider evaluation fixtures and a `code_to_query:provider:evaluate` task for tracking planner/provider safety regressions.
- Added a security policy and threat model covering safe deployment defaults, cross-tenant data exposure, schema-context filtering, observability data exposure, and vulnerability reporting.

### Changed
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
