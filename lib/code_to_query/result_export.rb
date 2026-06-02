# frozen_string_literal: true

require 'csv'
require 'json'

module CodeToQuery
  # Shapes already-materialized database result objects for presentation/export.
  #
  # ResultExport does not execute queries itself. Pass already-materialized
  # ActiveRecord::Result-like values that expose `columns` and `rows`, arrays of
  # hashes, or similar eager result objects whose `to_a` returns hash rows. Lazy
  # relations/scopes are not intended inputs because calling `to_a` may execute
  # them. Nested row values are passed through unchanged.
  module ResultExport
    module_function

    # Convert a result object into an array of hashes with string keys.
    def to_a(result)
      if columnar_result?(result)
        columns = result.columns.map(&:to_s)
        return [] if result.rows.nil?

        result.rows.map do |row|
          stringify_row(row, columns)
        end
      else
        result.to_a.map { |row| stringify_row(row) }
      end
    end

    # Convert a result object into a JSON array string.
    def to_json(result, options = nil)
      options ? JSON.generate(to_a(result), options) : JSON.generate(to_a(result))
    end

    # Convert a result object into a CSV string. Headers are ordered by the
    # result's `columns` when available, otherwise by the first occurrence of
    # each key across all shaped rows.
    def to_csv(result, **csv_options)
      rows = to_a(result)
      headers = csv_headers(result, rows)
      return '' if headers.empty? && rows.empty?

      CSV.generate(**csv_options) do |csv|
        csv << headers
        rows.each do |row|
          csv << headers.map { |header| row[header] }
        end
      end
    end

    def columnar_result?(result)
      result.respond_to?(:columns) && result.respond_to?(:rows)
    end
    private_class_method :columnar_result?

    def csv_headers(result, rows)
      if result.respond_to?(:columns)
        result.columns.map(&:to_s)
      elsif rows.any?
        rows.each_with_object([]) do |row, headers|
          row.each_key do |key|
            header = key.to_s
            headers << header unless headers.include?(header)
          end
        end
      else
        []
      end
    end
    private_class_method :csv_headers

    def stringify_row(row, columns = nil)
      if row.respond_to?(:to_hash)
        row.to_hash.transform_keys(&:to_s)
      elsif columns
        columns.zip(Array(row)).to_h
      else
        raise ArgumentError, 'result rows must be hashes unless columns are provided'
      end
    end
    private_class_method :stringify_row
  end
end
