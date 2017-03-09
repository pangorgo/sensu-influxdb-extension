require 'rubygems' if RUBY_VERSION < '1.9.0'
require 'em-http-request'
require 'eventmachine'
require 'multi_json'

module Sensu::Extension
  class InfluxDB < Handler
    def name
      definition[:name]
    end

    def definition
      {
        type: 'extension',
        name: 'influxdb'
      }
    end

    def description
      'Outputs metrics to InfluxDB'
    end

    def post_init()
      @influx_conf = parse_settings
      @influx_conf.keys.each do |instance|
        logger.info("InfluxDB extension initialiazed using instance named #{instance} #{@influx_conf[instance]['protocol'] }://#{ @influx_conf[instance]['host'] }:#{ @influx_conf[instance]['port'] } - Defaults : db=#{@influx_conf[instance]['database']} precision=#{@influx_conf[instance]['time_precision']}")
        @buffer[instance] = {}
        @flush_timer[instance] = EventMachine::PeriodicTimer.new(@influx_conf[instance]['buffer_max_age'].to_i) do
          unless buffer_size(instance) == 0
            logger.debug("InfluxDB instance #{ instance } cache age > #{@influx_conf[instance]['buffer_max_age']} : forcing flush")
            flush_buffer(instance)
          end
        end
        logger.info("InfluxDB instance #{ instance } write buffer initiliazed : buffer flushed every #{@influx_conf[instance]['buffer_max_size']} points OR every #{@influx_conf[instance]['buffer_max_age']} seconds) ")
      end
    end

    def run(event_data)

      @buffer.keys.each do |instance|
        event = parse_event(event_data)
        # init event and check data
        body = []
        client = event[:client][:name]
        event[:check][:influxdb][:database] ||= @influx_conf[instance]['database']
        event[:check][:time_precision] ||= @influx_conf[instance]['time_precision']

        event[:check][:output].split(/\n/).each do |line|
          key, value, time = line.split(/\s+/)
          values = "value=#{value.to_f}"

          if event[:check][:duration]
            values += ",duration=#{event[:check][:duration].to_f}"
          end

          if @influx_conf[instance]['strip_metric'] == 'host'
            key = slice_host(key, client)
          elsif @influx_conf[instance]['strip_metric']
            key.gsub!(/^.*#{@influx_conf[instance]['strip_metric']}\.(.*$)/, '\1')
          end

          # Avoid things break down due to comma in key name
          key.gsub!(',', '\,')
          key.gsub!(/\s/, '\ ')
          key.gsub!('"', '\"')
          key.gsub!("\\"){ "\\\\" }

          # This will merge : default conf tags < check embedded tags < sensu client/host tag
          tags = @influx_conf[instance]['tags'].merge(event[:check][:influxdb][:tags]).merge({'host' => client})
          tags.each do |tag, val|
            key += ",#{tag}=#{val}"
          end

          @buffer[instance][event[:check][:influxdb][:database]] ||= {}
          @buffer[instance][event[:check][:influxdb][:database]][event[:check][:time_precision]] ||= []

          @buffer[instance][event[:check][:influxdb][:database]][event[:check][:time_precision]].push([key, values, time.to_i].join(' '))
          flush_buffer(instance) if buffer_size(instance) >= @influx_conf[instance]['buffer_max_size']
        end
      end

      yield('', 0)
    end

    def stop
      @buffer.keys.each do |instance|
        logger.info("Flushing InfluxDB buffer before exiting for instance #{ instance }")
        flush_buffer(instance)
      end
      true
    end

    private

    def flush_buffer(instance)
      @flush_timer[instance].cancel
      logger.debug("Flushing InfluxDB buffer for instance #{ instance }")
      @buffer[instance].each do |db, tp|
        tp.each do |p, points|
          logger.debug("Sending #{ points.length } points to #{ db } InfluxDB database with precision=#{ p }")

          EventMachine::HttpRequest.new("#{ @influx_conf[instance]['protocol'] }://#{ @influx_conf[instance]['host'] }:#{ @influx_conf[instance]['port'] }/write?db=#{ db }&precision=#{ p }&u=#{ @influx_conf[instance]['username'] }&p=#{ @influx_conf[instance]['password'] }").post :body => points.join("\n")

        end
        logger.debug("Cleaning buffer for instance #{instance} db #{ db }")
        @buffer[instance][db] = {}
      end
    end

    def buffer_size(instance)
      sum = @buffer[instance].map { |_db, tp| tp.map { |_p, points| points.length}.inject(:+) }.inject(:+)
      return sum || 0
    end

    def parse_event(event_data)
      begin
        event = MultiJson.load(event_data)

        # default values
        # n, u, ms, s, m, and h (default community plugins use standard epoch date)
        event[:check][:time_precision] ||= nil
        event[:check][:influxdb] ||= {}
        event[:check][:influxdb][:tags] ||= {}
        event[:check][:influxdb][:database] ||= nil

      rescue => e
        logger.error("Failed to parse event data: #{e}")
      end
      return event
    end

    def parse_settings()
      begin
        settings = @settings

        settings.keys.each do |key|
          # default values
          settings[key]['tags'] ||= {}
          settings[key]['use_ssl'] ||= false
          settings[key]['time_precision'] ||= 's'
          settings[key]['protocol'] = settings['use_ssl'] ? 'https' : 'http'
          settings[key]['buffer_max_size'] ||= 500
          settings[key]['buffer_max_age'] ||= 6 # seconds
          settings[key]['port'] ||= 8086
        end

      rescue => e
        logger.error("Failed to parse InfluxDB settings #{e}")
      end
      return settings
    end

    def slice_host(slice, prefix)
      prefix.chars.zip(slice.chars).each do |char1, char2|
        break if char1 != char2
        slice.slice!(char1)
      end
      slice.slice!('.') if slice.chars.first == '.'
      return slice
    end

    def logger
      Sensu::Logger.get
    end
  end
end
