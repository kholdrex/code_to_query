# frozen_string_literal: true

require 'net/http'
require 'uri'
require 'json'

module CodeToQuery
  class LLMClient
    def initialize(config)
      @config = config
    end

    # messages: [{ role: 'system'|'user'|'assistant', content: '...' }, ...]
    # options: extra provider-specific options to merge into payload
    # Returns assistant message content (String) or nil
    def chat(messages:, options: {})
      base = (@config.llm_api_base || 'https://api.openai.com/v1').to_s
      uri = URI("#{base.chomp('/')}/chat/completions")

      payload = {
        model: @config.openai_model,
        messages: messages,
        temperature: @config.llm_temperature
      }.merge(@config.provider_options || {}).merge(options || {})

      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = (uri.scheme == 'https')
      http.read_timeout = @config.llm_timeout

      request = Net::HTTP::Post.new(uri)
      request['Authorization'] = "Bearer #{@config.openai_api_key}" if @config.openai_api_key
      request['Content-Type'] = 'application/json'
      request.body = payload.to_json

      response = http.request(request)
      unless response.is_a?(Net::HTTPSuccess)
        raise CodeToQuery::APIError, "LLM API error: #{response.code} #{response.message}"
      end

      body = JSON.parse(response.body)
      body.dig('choices', 0, 'message', 'content')
    rescue StandardError => e
      raise CodeToQuery::APIError, e.message
    end
  end
end
