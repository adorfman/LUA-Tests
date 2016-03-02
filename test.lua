#!/usr/local/bin/lua


require 'pl'
require "luasql.sqlite3"

function insert_record ( con, record ) 

   local sql = string.format( [[INSERT INTO records values("%s","%s","%s","%s")]],  
                              record[1], record[2], record[3], record[4] )


   local res,err = assert( con:execute(sql) )

end

function string:split( splitpattern )
 
   local results = {}
   local index   = 1
   local start   = 1

   local splitstart, splitend = string.find( self, splitpattern, start )

   while splitstart do
      results[index] = string.sub( self, start, splitstart - 1 ) 

      index = index + 1 
      start = splitend + 1

      splitstart, splitend = string.find( self, splitpattern, start )
   end

   -- deal with trailing commas
   local last = string.sub( self, start )  
   if string.len(last) > 0 then 
       results[index] = last 
   end

   return results
end

local env   = luasql.sqlite3() 
local dbcon = env:connect("test.db") 
local createtablestr =  [[  
CREATE TABLE IF NOT EXISTS records( 
  id     int,
  name   varchar(20), 
  number int(10), 
  job    varchar(50)
)]]

res,err = assert( dbcon:execute(createtablestr) )


file = assert( io.open("test.csv", "r") )

io.input(file)

local headers = io.read("*line")

while io.read(0) do

  local line = io.read("*line")

  -- skip empty lines
  if string.find( line, "%w" ) then 

    local fields = line:split(',%s*')
    pretty.dump(fields)

    insert_record( dbcon, fields )

  end

end

cur = dbcon:execute("select count(id) from records") 

key,msg = cur:fetch();

print( string.format( "Inserted %s records\n", key ) )

dbcon:execute("delete from records") 

io.close(file)
cur:close()
dbcon:close()
env:close()

