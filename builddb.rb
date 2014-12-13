require 'sqlite3'
require 'json'

ROOT_DIR = File.dirname __FILE__
DB_FILE = "#{ROOT_DIR}/mav.db"
JSON_DIR = "#{ROOT_DIR}/json"

db = SQLite3::Database.new DB_FILE

db.execute <<-SQL
    DROP TABLE IF EXISTS TRAIN
SQL

db.execute <<-SQL
    CREATE TABLE train(
        datetime DATETIME,
        train_number VARCHAR(10),
        lat VARCHAR(20),
        lon VARCHAR(20)
    )
SQL

json_files = Dir["#{JSON_DIR}/*.json"]
current_file = 1
json_files.each do |filename|
    print "\r[Processing file #{filename} (#{current_file}/#{json_files.length})]"

    file = File.open(filename, 'r')
    content = file.read
    file.close

    begin
        json = JSON.parse content
        time = json['@CreationTime']

        json['Trains']['Train'].each do |train|
            db.execute <<-SQL, [time, train['@TrainNumber'], train['@Lat'], train['@Lon']]
                INSERT INTO train(datetime, train_number, lat, lon) VALUES (
                    ?, ?, ?, ?
                )
            SQL
        end
    rescue StandardError => e
        puts "File #{filename}: error: #{e} => skipping"
    end

    current_file += 1
end

puts
