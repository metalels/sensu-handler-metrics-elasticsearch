#!/usr/bin/env ruby
#
# Sensu Elasticsearch Metrics Handler

# uses File.write
if RUBY_VERSION < '2.0.0'
  STDERR.puts "can't use this handler with ruby <= 1.9"
  exit 2;
end

require 'sensu-handler'
require 'net/http'
require 'timeout'
require 'digest/md5'
require 'date'
require 'base64'

class ElasticsearchMetrics < Sensu::Handler
  def host
    settings['elasticsearch-metrics']['host'] || 'localhost'
  end

  def port
    settings['elasticsearch-metrics']['port'] || 9200
  end

  def es_index
    settings['elasticsearch-metrics']['index'] || 'sensu-metrics'
  end

  def es_id
    rdm = ((0..9).to_a + ("a".."z").to_a + ("A".."Z").to_a).sample(3).join
    Digest::MD5.new.update("#{rdm}")
  end

  def time_stamp
    d = DateTime.now
    d.to_s
  end

  def event_name
    @event['client']['name'] + '/' + @event['check']['name']
  end

  def check_es_type
    @event['check']['name']
  end

  def cache_root
    @event['check']['cache'] || "/tmp/sensu/"
  end

  def cache_path(key)
    "#{cache_root}/#{Base64.encode64(key)}"
  end

  def read_cache(key)
    File.read cache_path(key)
  rescue
    nil
  end

  def write_cache(key, val)
    File.write cache_path(key), val
  rescue
    nil
  end

  def handle
    metrics ={}
    @event['check']['output'].split("\n").each do |line|
      line = line.chomp.strip
      v = line =~ /\t/ ? line.split("\t") : line.split("\s")
      unless v[1]
        puts "elasticsearch parse error."
        next
      end
      cache = read_cache v[0]
      next unless write_cache v[0], v[1] # have to after read_cache
      next unless cache # have to after write_cache
      v[1] = v[1].to_i - cache.to_i
      if v[1] < 0 # counter has cleard
        # select 32bit or 64bit from cache value
        v[1] += cache.to_i > 4294967925 ? 18446744073709551615 : 4294967925
      end

      metrics = {
        :@timestamp => time_stamp,
        :client => @event['client']['name'],
        :check_name => @event['check']['name'],
        :status => @event['check']['status'],
        :address => @event['client']['address'],
        :command => @event['check']['command'],
        :occurrences => @event['occurrences'],
        :key => v[0],
        :value => v[1]
      }
      begin
        Timeout.timeout(5) do
          uri = URI("http://#{host}:#{port}/#{es_index}/#{check_es_type}/#{es_id}")
          http = Net::HTTP.new(uri.host, uri.port)
          request = Net::HTTP::Post.new(uri.path, "content-type" => "application/json; charset=utf-8")
          request.body = JSON.dump(metrics)

          response = http.request(request)
          case response.code.to_i
          when 200, 201
            puts "request metrics #=> #{metrics}"
            puts "request body #=> #{response.body}"
            puts "elasticsearch post ok."
          else
            puts "request metrics #=> #{metrics}"
            puts "request body #=> #{response.body}"
            puts "elasticsearch post failure. status error code #=> #{response.code}"
          end
        end
      rescue Timeout::Error
        puts "elasticsearch timeout error."
      end
    end
  end
end
