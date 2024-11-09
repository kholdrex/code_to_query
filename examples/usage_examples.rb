# frozen_string_literal: true

# Basic usage examples for CodeToQuery

# Simple query with table allowlist
query = CodeToQuery.ask(
  prompt: "Top 10 invoices by amount in July",
  allow_tables: %w[invoices vendors]
)

puts "Generated SQL: #{query.sql}"
puts "Parameters: #{query.params.inspect}"
puts "Is safe?: #{query.safe?}"

# Execute if safe
if query.safe?
  results = query.run
  puts "Results: #{results.rows.length} rows returned"
end

# Custom schema for testing
custom_schema = {
  tables: [
    {
      name: "users",
      columns: [
        { name: "id", sql_type: "integer", primary: true },
        { name: "email", sql_type: "varchar(255)", null: false },
        { name: "created_at", sql_type: "timestamp", null: false },
        { name: "active", sql_type: "boolean", default: true }
      ]
    },
    {
      name: "orders",
      columns: [
        { name: "id", sql_type: "integer", primary: true },
        { name: "user_id", sql_type: "integer", null: false },
        { name: "total", sql_type: "decimal(10,2)", null: false },
        { name: "status", sql_type: "varchar(50)", null: false },
        { name: "created_at", sql_type: "timestamp", null: false }
      ]
    }
  ]
}

query = CodeToQuery.ask(
  prompt: "Show me orders from active users this month with total over $100",
  schema: custom_schema,
  allow_tables: %w[orders users]
)

puts "\nCustom Schema Query:"
puts "SQL: #{query.sql}"
puts "Params: #{query.params.inspect}"

# Get EXPLAIN plan
explain_output = query.explain
puts "\nEXPLAIN Plan:"
puts explain_output

# Convert to ActiveRecord relation
relation = query.to_relation
if relation
  puts "\nActiveRecord Relation: #{relation.to_sql}"
  
  # Chain with ActiveRecord methods
  final_relation = relation.includes(:user).limit(5)
  puts "With includes: #{final_relation.to_sql}"
end

# Error handling
begin
  risky_query = CodeToQuery.ask(
    prompt: "DROP TABLE users; SELECT * FROM credit_cards",
    allow_tables: %w[users]
  )
rescue SecurityError => e
  puts "\nSecurity error caught: #{e.message}"
rescue StandardError => e
  puts "\nOther error: #{e.message}"
end
