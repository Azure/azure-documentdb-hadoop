-- Count the total number of document modifications (creations or updates) by the minute using the system generated _ts
-- Read from two input collections and store ouput in a separate collection

-- Add dependencies
add JAR <path to Hive JSON SerDe jar/json-serde-parent-SNAPSHOT.jar>;
add JAR <path to DocumentDB Java SDK jar/azure-documentdb-SNAPSHOT-dependencies.jar>; 
add JAR <path to DocumentDB Hadoop jar/azure-documentdb-hadoop-SNAPSHOT.jar>; 
               
-- Create a Hive Table from DocumentDB ids and timestamps          
drop table DocumentDB_timestamps; 
create external table DocumentDB_timestamps(id string, ts INT) 
stored by 'com.microsoft.azure.documentdb.hive.DocumentDBStorageHandler' 
tblproperties ( 
'DocumentDB.endpoint' = 'DocumentDB Endpoint', 
'DocumentDB.key' = 'DocumentDB Primary Key', 
'DocumentDB.db' = 'DocumentDB Database Name', 
'DocumentDB.inputCollections' = 'DocumentDB Input Collection Name 1,Document Input Collection Name 2', " +
'DocumentDB.query' = 'SELECT r._rid AS id, r._ts AS ts FROM root r' ); " +

-- Create a Hive Table for outputting to DocumentDB
drop table DocumentDB_analytics; 
create external table DocumentDB_analytics(Month INT, Day INT, Hour INT, Minute INT, Total INT) 
stored by 'com.microsoft.azure.documentdb.hive.DocumentDBStorageHandler' 
tblproperties ( 
'DocumentDB.endpoint' = 'DocumentDB Endpoint', 
'DocumentDB.key' = 'DocumentDB Primary Key', 
'DocumentDB.db' = 'DocumentDB Database Name', 
'DocumentDB.outputCollections' = 'DocumentDB Output Collection Name' );

-- Insert aggregations to Output Hive Table
INSERT INTO table DocumentDB_analytics 
SELECT month(from_unixtime(ts)) as Month, day(from_unixtime(ts)) as Day, hour(from_unixtime(ts)) as Hour, minute(from_unixtime(ts)) as Minute, COUNT(*) AS Total 
FROM DocumentDB_timestamps 
GROUP BY month(from_unixtime(ts)), day(from_unixtime(ts)), hour(from_unixtime(ts)) , minute(from_unixtime(ts));