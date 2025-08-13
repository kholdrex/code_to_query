# Changelog

This file tracks the major changes in each release.

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
