require 'sqlite3'
require 'json'

ROOT_DIR = File.dirname __FILE__
DB_FILE = "#{ROOT_DIR}/mav.db"
JSON_DIR = "#{ROOT_DIR}/json"

OPT_REBUILD = ARGV.include? '--rebuild'

db = SQLite3::Database.new DB_FILE

if OPT_REBUILD
    ['train', 'loaded_file'].each do |table|
        db.execute <<-SQL
            DROP TABLE IF EXISTS #{table}
        SQL
    end
end

db.execute <<-SQL
    CREATE TABLE IF NOT EXISTS loaded_file(
        filename VARCHAR(64)
    )
SQL

db.execute <<-SQL
    CREATE TABLE IF NOT EXISTS train(
        datetime DATETIME,
        train_number VARCHAR(10),
        lat VARCHAR(20),
        lon VARCHAR(20),
        delay INT
    )
SQL

start_time = Time.new.to_i
json_files = Dir["#{JSON_DIR}/*.json"]
current_file = 1

json_files.each do |filename|
    current_time = Time.new.to_i
    elapsed_time = current_time - start_time
    seconds_remaining = elapsed_time.to_f / current_file * (json_files.length - current_file)

    print "\r[Processing file #{filename} (#{current_file}/#{json_files.length}) " +
            "(ETA: #{seconds_remaining.to_i}s)]"

    rows = db.execute <<-SQL, [filename]
        SELECT 1 FROM loaded_file WHERE filename = ?
    SQL

    if rows.length == 0
        file = File.open(filename, 'r')
        content = file.read
        file.close

        begin
            json = JSON.parse content
            time = json['@CreationTime'].gsub '.', '-'

            json['Trains']['Train'].each do |train|
                db.execute <<-SQL, [time, train['@TrainNumber'], train['@Lat'], train['@Lon'], train['@Delay']]
                    INSERT INTO train(datetime, train_number, lat, lon, delay) VALUES (
                        ?, ?, ?, ?, ?
                    )
                SQL
            end
        rescue StandardError => e
            puts "File #{filename}: error: #{e} => skipping"
        end

        db.execute <<-SQL, [filename]
            INSERT INTO loaded_file(filename) VALUES(?)
        SQL
    end

    current_file += 1
end

puts
