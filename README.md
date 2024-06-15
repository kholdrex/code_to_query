# CodeToQuery

A gem that converts natural language questions into SQL queries for Rails apps. It's built for teams who need to give non-developers access to data without compromising security or performance.

## What it does

Instead of writing SQL, your team can ask questions like "Show me top customers by revenue this month" and get back safe, parameterized queries that respect your database policies and security rules.

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
  config.openai_model = 'gpt-4'
  
  # Security settings
  config.enable_explain_gate = true
  config.allow_seq_scans = false
  config.max_query_cost = 10000
  config.require_limit_by_default = true
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
config.enable_explain_gate = true    # Block expensive queries
config.allow_seq_scans = false       # Prevent table scans
config.max_query_cost = 10000        # Cost threshold
config.max_joins = 3                 # Join limit
```

### OpenAI settings
```ruby
config.openai_api_key = ENV['OPENAI_API_KEY']
config.openai_model = 'gpt-4'
config.stub_llm = false              # Set true for testing
```

## Rake tasks

```bash
rails code_to_query:bootstrap    # Generate full context pack
rails code_to_query:schema       # Extract schema info
rails code_to_query:scan_app     # Scan models and associations
rails code_to_query:verify       # Check context pack integrity
```

## Security features

- **SQL injection prevention**: All queries are parameterized
- **Access control**: Table allowlists and row-level policies
- **Performance limits**: EXPLAIN plan analysis and cost thresholds
- **Readonly execution**: Uses dedicated readonly database connections

## Advanced usage

### Custom policies
```ruby
config.policy_adapter = ->(user) do
  return {} unless user
  
  {
    company_id: user.company_id,
    user_id: user.admin? ? nil : user.id
  }
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
