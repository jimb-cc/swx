require 'httparty'
require 'mongo'
require 'slop'
require 'awesome_print'

opts = Slop.parse do |o|
    o.string '-h', '--host', 'the connection string for the MongoDB cluster (default: localhost)', default: 'mongodb://localhost'
    o.string '-d', '--database', 'the database to use (default: ts)', default: 'ts'
    o.string '-c', '--collection', 'the (timeseries) collection to use (default: ts)', default: 'ts'
    o.string '-u', '--url', 'the URL of the API endpoint (default: GOES 6-hour xrays)', default: 'https://services.swpc.noaa.gov/json/goes/primary/xrays-6-hour.json'
  end

# set the logger level for the mongo driver
Mongo::Logger.logger.level = ::Logger::WARN
puts "Connecting to #{opts[:host]}, and db #{opts[:database]}"
DB = Mongo::Client.new(opts[:host], database: opts[:database])


# Issue an administrative command
#ap DB.database.command(dbstats: 1)


DB[opts[:collection]].indexes.create_one(
    { time_tag: 1, energy: 1},
    name: 'ix_tt_e',
    unique: true
)

DB[opts[:collection]].indexes.each do |i|
    ap i
end

def pushData(db,coll)

    url = 'https://services.swpc.noaa.gov/json/goes/primary/xrays-6-hour.json'
    response = HTTParty.get(url)
    response.parsed_response

    count = 0
    response.parsed_response.each do |doc,i|
        ts = doc["time_tag"]
        fixedTS = DateTime.parse(doc["time_tag"])
        doc[:"time_tag"] = fixedTS
        result = db[coll].insert_one(doc)
        putc "."
        count = count +1
    rescue
        putc "x"    
    end
    puts "\nInserted #{count} new measurements from #{response.parsed_response.length} results"
end

pushData(DB,opts[:collection])    