require 'httparty'

class MavApi
    include HTTParty
    base_uri 'http://vonatinfo.mav-start.hu'

    def initialize
        @options = {
            :headers => {
                'Content-Type' => 'application/json; charset=UTF-8'
            }
        }
    end

    def get_trains
        result = self.class.post('/map.aspx/getData', with_json({
            :a => 'TRAINS',
            :jo => {
                :history => false,
                :id => nil
            }
        }))
        result['d']['result']
    end

    private

    def with_json(json)
        @options.merge :body => json.to_json
    end
end

REFRESH_FREQUENCY = 30
JSON_DIR = "#{File.dirname __FILE__}/json"

def next_second(start, current)
    start + (((current - start) / REFRESH_FREQUENCY).to_i + 1) * REFRESH_FREQUENCY
end

start_timestamp = Time.new.to_i

if not File.exists?(JSON_DIR)
    Dir.mkdir(JSON_DIR)
end


while true do
    print '[Requesting fresh data....]'
    trains = MavApi.new.get_trains
    current_timestamp = Time.new.to_i

    filename = "#{current_timestamp}.json"
    print "[Writing data to json/#{filename}]"
    json_file = File.new("#{JSON_DIR}/#{filename}", 'w')
    json_file.write(trains.to_json)
    json_file.close()

    sleep_seconds = next_second(start_timestamp, current_timestamp) - current_timestamp
    print "[Sleeping for #{sleep_seconds} seconds]\n"
    sleep sleep_seconds
end
