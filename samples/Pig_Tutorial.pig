-- Count the total number of document modifications (creations or updates) by the minute using the system generated _ts
-- Read from two input collections and store ouput in a separate collection

-- Add dependencies
REGISTER <path to DocumentDB Java SDK jar/azure-documentdb-SNAPSHOT-dependencies.jar>; 
REGISTER <path to DocumentDB Hadoop jar/azure-documentdb-hadoop-SNAPSHOT.jar>; 

-- Load DocumentDB ids and timestamps   
DocumentDB_timestamps = LOAD 'DocumentDB Endpoint' USING com.microsoft.azure.documentdb.pig.DocumentDBLoader( 
'DocumentDB Primary Key', 'DocumentDB Database Name', 'DocumentDB Input Collection Name 1,DocumentDB Input Collection Name 2', 
'SELECT r._rid AS id, r._ts AS ts FROM root r' ); 

timestamp_record = FOREACH DocumentDB_timestamps GENERATE $0#'id' as id:int, ToDate((long)($0#'ts') * 1000) as timestamp:datetime; 

by_minute = GROUP timestamp_record BY (GetYear(timestamp), GetMonth(timestamp), GetDay(timestamp), GetHour(timestamp), GetMinute(timestamp)); 
by_minute_count = FOREACH by_minute GENERATE FLATTEN(group) as (Year:int, Month:int, Day:int, Hour:int, Minute:int), COUNT(timestamp_record) as Total:int; 

-- Store results back into DocumentDB               
STORE by_minute_count INTO 'DocumentDB Endpoint' 
USING com.microsoft.azure.documentdb.pig.DocumentDBStorage( 
'DocumentDB Primary Key', 'DocumentDB Database Name', 'DocumentDB Output Collection Name');
