# frozen_string_literal: true

require 'httparty'
require 'mongo'
require 'slop'

opts = Slop.parse do |o|
  o.string  '-h', '--host', 'the URI for the MongoDB cluster (default: localhost)', default: 'mongodb://localhost'
  o.string  '-d', '--database', 'the database to use (default: ts)', default: 'ts'
  o.string  '-c', '--collection', 'the (timeseries) collection to use (default: ts)', default: 'ts'
  o.string  '-l', '--logcollection', 'the collection to use for logging (default: log)', default: 'log'
  o.string  '-u', '--url', 'the URL of the API endpoint (default: GOES 6-hour xrays)', default: 'https://services.swpc.noaa.gov/json/goes/primary/xrays-6-hour.json'
  o.integer '-s', '--sleep', 'time to sleep between requests (default: 60)', default: 60
end

# set the logger level for the mongo driver
Mongo::Logger.logger.level = ::Logger::WARN
puts "Connecting to #{opts[:host]}, and db #{opts[:database]}"
DB = Mongo::Client.new(opts[:host], database: opts[:database])

#DB[opts[:collection]].indexes.create_one(
#  { time_tag: 1, energy: 1 },
#  name: 'ix_tt_e'
#    unique: true
#)

DB[opts[:collection]].indexes.each do |i|
  puts i
end

def pushData(db, coll, url, logcollection)
  response = HTTParty.get(url)
  response.parsed_response

  count = 0
  now = Time.now
  response.parsed_response.each do |doc, _i|
    fixedTS = DateTime.parse(doc['time_tag']).to_time
    doc['time_tag'] = fixedTS
    numDocs = db[coll].find(time_tag: doc['time_tag'], energy: doc['energy']).count_documents
    if numDocs.zero?
      putc '!'
      result = db[coll].insert_one(doc)
      count += 1
    else
      putc '.'
    end
  rescue StandardError
    putc 'x'
  end
  duration = (Time.now - now).round(2)
  log = { 'inserted' => count, 'recieved' => response.parsed_response.length, 'duration' => duration, 'ts' => Time.now }
  puts "\nInserted #{count} new measurements from #{response.parsed_response.length} results in #{duration} seconds"
  DB[logcollection].insert_one(log)
end

loop do
  pushData(DB, opts[:collection], opts[:url], opts[:logcollection])
  puts "going to sleep for #{opts[:sleep]} seconds....\n\n"
  sleep(opts[:sleep])
end
