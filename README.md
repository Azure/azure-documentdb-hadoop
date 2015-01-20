#Microsoft Azure DocumentDB Hadoop Connector

![](https://img.shields.io/github/release/azure/azure-documentdb-hadoop.svg)
![](https://img.shields.io/github/issues/azure/azure-documentdb-hadoop.svg)

This project provides a client library in Java that allows Microsoft Azure DocumentDB to act as an input source or output sink for MapReduce, Hive, and Pig jobs.

##Download
###Option 1: Via Github

To get the binaries of this library as distributed by Microsoft, ready for use within your project, you can use Github releases.

###Option 2: Source Via Git

To get the source code of the connector via git just type:

    git clone git://github.com/Azure/azure-documentdb-hadoop.git

###Option 3: Source Zip

To download a copy of the source code, click "Download ZIP" on the right side of the page or click [here](https://github.com/Azure/azure-documentdb-hadoop/archive/master.zip). 

##Requirements
* Java Development Kit 7

##Supported Versions
* Apache Hadoop & YARN 2.4.0
    * Apache Pig 0.12.1
    * Apache Hive & HCatalog 0.13.1
    * HDI 3.1

## Dependencies
* Microsoft Azure DocumentDB Java SDK 0.9.3 (com.microsoft.azure / azure-documentdb / 0.9.3)
</br>
When using Hive:
* OpenX Technologies JsonSerde 1.3.1-SNAPSHOT (org.openx.data / json-serde-parent / 1.3.1-SNAPSHOT)
</br>
GitHub repo [here](https://github.com/rcongiu/Hive-JSON-Serde)

Please download the jars and add them to your build path. 

##Usage

To use this client library with Azure DocumentDB, you need to first [create an account](http://azure.microsoft.com/en-us/documentation/articles/documentdb-create-account/).

###MapReduce

#####Configuring input and output from DocumentDB Example
<p>
<d1>
<dt>*// Import Hadoop Connector Classes*</dt>
<dt>**import** com.microsoft.azure.documentdb.Document;</dt>
<dt>**import** com.microsoft.azure.documentdb.hadoop.ConfigurationUtil;</dt>
<dt>**import** com.microsoft.azure.documentdb.hadoop.DocumentDBInputFormat;</dt>
<dt>**import** com.microsoft.azure.documentdb.hadoop.DocumentDBOutputFormat;</dt>
<dt>**import** com.microsoft.azure.documentdb.hadoop.DocumentDBWritable;</dt>
</d1>
</p>
<p>
<d1>
<dt>*// Set Configurations*</dt>
<dt>**Configuration** conf = new Configuration();</dt>
<dt>**final String** host = "*Your DocumentDB Endpoint*";</dt>
<dt>**final String** key = "*Your DocumentDB Primary Key*";</dt>
<dt>**final String** dbName = "*Your DocumentDB Database Name*";</dt>
<dt>**final String** inputCollNames = "*Your DocumentDB Input Collection Name[s]*";</dt>
<dt>**final String** outputCollNames = "*Your DocumentDB Output Collection Name[s]*";</dt>
<dt>**final String** query = "*Your DocumentDB Query*";</dt>
<br/>
<dt>conf.set(ConfigurationUtil.DB_HOST, host);</dt>
<dt>conf.set(ConfigurationUtil.DB_KEY, key);</dt>
<dt>conf.set(ConfigurationUtil.DB_NAME, dbName);</dt>
<dt>conf.set(ConfigurationUtil.INPUT_COLLECTION_NAMES, inputCollNames);</dt>
<dt>conf.set(ConfigurationUtil.OUTPUT_COLLECTION_NAMES, outputCollNames);</dt>
<dt>conf.set(ConfigurationUtil.QUERY, query);</dt>
</d1>
</p>

Full MapReduce sample can be found [here]().

###Hive
#####Loading data from DocumentDB Example
<p>
<d1>
<dt>**CREATE EXTERNAL TABLE** *DocumentDB_Hive_Table*( *COLUMNS* ) </dt>
<dt>**STORED BY** 'com.microsoft.azure.documentdb.hive.DocumentDBStorageHandler' </dt>
<dt>tblproperties ( </dt>
<dd>'DocumentDB.endpoint' = '*Your DocumentDB Endpoint*', </dd>
<dd>'DocumentDB.key' = '*Your DocumentDB Primary Key*', </dd>
<dd>'DocumentDB.db' = '*Your DocumentDB Database Name*', </dd>
<dd>'DocumentDB.inputCollections' = '*Your DocumentDB Input Collection Name[s]*', </dd>
<dd>'[Optional] DocumentDB.query' = '[Optional] *Your DocumentDB Query*' );</dd>
</d1>
</p>

#####Storing data to DocumentDB Example
<p>
<d1>
<dt>**CREATE EXTERNAL TABLE** *Hive_DocumentDB_Table*( *COLUMNS* )</dt> 
<dt>**STORED BY** 'com.microsoft.azure.documentdb.hive.DocumentDBStorageHandler' </dt>
<dt>tblproperties ( </dt> 
<dd>'DocumentDB.endpoint' = '*Your DocumentDB Endpoint*', </dd>
<dd>'DocumentDB.key' = '*Your DocumentDB Primary Key*', </dd>
<dd>'DocumentDB.db' = '*Your DocumentDB Database Name*', </dd>
<dd>'DocumentDB.outputCollections' = '*Your DocumentDB Output Collection Name[s]*' ); </dd>
<dt>**INSERT INTO TABLE** *Hive_DocumentDB_Table* </dt>
</d1>
</p>

Full Hive sample can be found [here]().

###Pig
#####Loading data from DocumentDB Example
<p>
<d1>
<dt>**LOAD** '*Your DocumentDB Endpoint*' </dt>
<dt>**USING** com.microsoft.azure.documentdb.hadoop.pig.DocumentDBLoader( </dt>
<dd>'*Your DocumentDB Primary Key*', </dd> 
<dd>'*Your DocumentDB Database Name*',</dd> 
<dd>'*Your DocumentDB Input Collection Name[s]*',</dd>
<dd>'[Optional] *Your DocumentDB SQL Query*' ); </dd> 
</d1>
</p>

#####Storing data to DocumentDB Example
<p>
<d1>
<dt>**STORE** *data*  **INTO** '*DocumentDB Endpoint*' </dt> 
<dt>**USING** com.microsoft.azure.documentdb.hadoop.pig.DocumentDBStorage( </dt>
<dd>'*DocumentDB Primary Key*',</dd> 
<dd>'*DocumentDB Database Name*',</dd> 
<dd>'*DocumentDB Output Collection Name[s]*' ); </dd>
</d1>
</p>

Full Pig sample can be found [here]().

##Remarks
* When outputting to DocumentDB, your output collection will require capacity for an additional stored procedure. The stored procedure will remain in your collection for reuse.
* You must set range indices for your collection if you are pushing range queries to DocumentDB. More information can be found [here](http://azure.microsoft.com/en-us/documentation/articles/documentdb-indexing-policies/).
* Connector supports configurable *upsert* option. *Upsert* configuration is automatically set to *true* and will overwrite documents within the same collection with the same *id*.
* Reads and writes to DocumentDB will be counted against your provisioned throughput.

##Need Help?

Be sure to check out the Microsoft Azure [Developer Forums on MSDN](https://social.msdn.microsoft.com/forums/azure/en-US/home?forum=AzureDocumentDB) or the [Developer Forums on Stack Overflow](http://stackoverflow.com/questions/tagged/azure-documentdb) if you have trouble with the provided code.

##Contribute Code or Provide Feedback

If you would like to become an active contributor to this project please follow the instructions provided in [Azure Projects Contribution Guidelines](http://azure.github.io/guidelines.html).

If you encounter any bugs with the library please file an issue in the [Issues](https://github.com/Azure/azure-documentdb-hadoop/issues) section of the project.

##Learn More
* [Official Hadoop Documentation](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html)
* [Azure Developer Center](http://azure.microsoft.com/en-us/develop/java/)
* [Azure DocumentDB Service](http://azure.microsoft.com/en-us/documentation/services/documentdb/)
* [Azure DocumentDB Team Blog](http://blogs.msdn.com/b/documentdb/)