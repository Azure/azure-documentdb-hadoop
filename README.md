#Microsoft Azure DocumentDB Hadoop Connector

![](https://img.shields.io/github/release/azure/azure-documentdb-hadoop.svg)
![](https://img.shields.io/github/issues/azure/azure-documentdb-hadoop.svg)

This project provides a client library in Java that allows Microsoft Azure DocumentDB to act as an input source or output sink for MapReduce, Hive, and Pig jobs.

##Download
###Option 1: Via Github

To get the binaries of this library as distributed by Microsoft, ready for use within your project, you can use [GitHub releases](https://github.com/Azure/azure-documentdb-hadoop/releases).

###Option 2: Source Via Git

To get the source code of the connector via git just type:

    git clone git://github.com/Azure/azure-documentdb-hadoop.git

###Option 3: Source Zip

To download a copy of the source code, click "Download ZIP" on the right side of the page or click [here](https://github.com/Azure/azure-documentdb-hadoop/archive/master.zip). 

###Option 4: Via Maven

To get the binaries of this library as distributed by Microsoft, ready for use within your project, you can use Maven. 

    <dependency>
    	<groupId>com.microsoft.azure</groupId>
    	<artifactId>azure-documentdb-hadoop</artifactId>
    	<version>0.9.1</version>
    </dependency>

##Requirements
* Java Development Kit 7

##Supported Versions
* Apache Hadoop & YARN 2.4.0
    * Apache Pig 0.12.1
    * Apache Hive & HCatalog 0.13.1
* HDI 3.1 ([Getting started with HDInsight](http://azure.microsoft.com/en-us/documentation/articles/hdinsight-get-started/))

## Dependencies
* Microsoft Azure DocumentDB Java SDK 0.9.4 (com.microsoft.azure / azure-documentdb / 0.9.4)

When using Hive:
* OpenX Technologies JsonSerde 1.3.1-SNAPSHOT (org.openx.data / json-serde-parent / 1.3.1-SNAPSHOT)
    GitHub repo can be found [here](https://github.com/rcongiu/Hive-JSON-Serde)

Please download the jars and add them to your build path. 

##Usage

To use this client library with Azure DocumentDB, you need to first [create an account](http://azure.microsoft.com/en-us/documentation/articles/documentdb-create-account/).

###MapReduce

#####Configuring input and output from DocumentDB Example
```Java
    // Import Hadoop Connector Classes
    import com.microsoft.azure.documentdb.Document;
    import com.microsoft.azure.documentdb.hadoop.ConfigurationUtil;
    import com.microsoft.azure.documentdb.hadoop.DocumentDBInputFormat;
    import com.microsoft.azure.documentdb.hadoop.DocumentDBOutputFormat;
    import com.microsoft.azure.documentdb.hadoop.DocumentDBWritable;

    // Set Configurations
    Configuration conf = new Configuration();
    final String host = "Your DocumentDB Endpoint";
    final String key = "Your DocumentDB Primary Key";
    final String dbName = "Your DocumentDB Database Name";
    final String inputCollNames = "Your DocumentDB Input Collection Name[s]";
    final String outputCollNames = "Your DocumentDB Output Collection Name[s]";
    final String query = "[Optional] Your DocumentDB Query";
    final String upsert = "[Optional] Bool to disable or enable document upsert";
    final String rangeindices = "[Optional] Your document property name[s] that should have a range index";

    conf.set(ConfigurationUtil.DB_HOST, host);
    conf.set(ConfigurationUtil.DB_KEY, key);
    conf.set(ConfigurationUtil.DB_NAME, dbName);
    conf.set(ConfigurationUtil.INPUT_COLLECTION_NAMES, inputCollNames);
    conf.set(ConfigurationUtil.OUTPUT_COLLECTION_NAMES, outputCollNames);
    conf.set(ConfigurationUtil.QUERY, query);
    conf.set(ConfigurationUtil.UPSERT, upsert);
    conf.set(ConfigurationUtil.OUTPUT_RANGE_INDEXED, rangeindices);
```

Full MapReduce sample can be found [here](https://github.com/Azure/azure-documentdb-hadoop/blob/master/samples/MapReduceTutorial.java).

###Hive
#####Loading data from DocumentDB Example
```Java
    CREATE EXTERNAL TABLE DocumentDB_Hive_Table( COLUMNS )
    STORED BY 'com.microsoft.azure.documentdb.hive.DocumentDBStorageHandler'
    tblproperties (
        'DocumentDB.endpoint' = 'Your DocumentDB Endpoint',
        'DocumentDB.key' = 'Your DocumentDB Primary Key',
        'DocumentDB.db' = 'Your DocumentDB Database Name',
        'DocumentDB.inputCollections' = 'Your DocumentDB Input Collection Name[s]',
        '[Optional] DocumentDB.query' = '[Optional] Your DocumentDB Query' );
```

#####Storing data to DocumentDB Example
```Java
    CREATE EXTERNAL TABLE Hive_DocumentDB_Table( COLUMNS )
    STORED BY 'com.microsoft.azure.documentdb.hive.DocumentDBStorageHandler' 
    tblproperties ( 
        'DocumentDB.endpoint' = 'Your DocumentDB Endpoint', 
        'DocumentDB.key' = 'Your DocumentDB Primary Key', 
        'DocumentDB.db' = 'Your DocumentDB Database Name', 
        'DocumentDB.outputCollections' = 'Your DocumentDB Output Collection Name[s]',
        '[Optional] DocumentDB.upsert' = '[Optional] Bool to disable or enable document upsert',
        '[Optional] DocumentDB.rangeIndex' = '[Optional] Your document property name[s] that should have a range index');
    INSERT INTO TABLE Hive_DocumentDB_Table
```
Full Hive sample can be found [here](https://github.com/Azure/azure-documentdb-hadoop/blob/master/samples/Hive_Tutorial.hql).

###Pig
#####Loading data from DocumentDB Example
```Java
    LOAD 'Your DocumentDB Endpoint' 
    USING com.microsoft.azure.documentdb.hadoop.pig.DocumentDBLoader( 
        'Your DocumentDB Primary Key', 
        'Your DocumentDB Database Name',
        'Your DocumentDB Input Collection Name[s]',
        '[Optional] Your DocumentDB SQL Query' );
```

#####Storing data to DocumentDB Example
```Java
    STORE data  INTO 'DocumentDB Endpoint' 
    USING com.microsoft.azure.documentdb.hadoop.pig.DocumentDBStorage( 
        'DocumentDB Primary Key',
        'DocumentDB Database Name',
        'DocumentDB Output Collection Name[s]',
        '[Optional] Bool to disable or enable document upsert',
        '[Optional] Your document property name[s] that should have a range index');
```
Full Pig sample can be found [here](https://github.com/Azure/azure-documentdb-hadoop/blob/master/samples/Pig_Tutorial.pig).

##Remarks
* When outputting to DocumentDB, your output collection will require capacity for an additional stored procedure. The stored procedure will remain in your collection for reuse.
* You must set range indices for your collection if you are pushing range queries to DocumentDB. More information can be found [here](http://azure.microsoft.com/en-us/documentation/articles/documentdb-indexing-policies/).
* Connector supports configurable *upsert* option. *Upsert* configuration is automatically set to *true* and will overwrite documents within the same collection with the same *id*.
* Reads and writes to DocumentDB will be counted against your provisioned throughput.
* Output to DocumentDB collections is done in batch round robin.

##Need Help?

Be sure to check out the Microsoft Azure [Developer Forums on MSDN](https://social.msdn.microsoft.com/forums/azure/en-US/home?forum=AzureDocumentDB) or the [Developer Forums on Stack Overflow](http://stackoverflow.com/questions/tagged/azure-documentdb) if you have trouble with the provided code. Also, check out our [tutorial](http://azure.microsoft.com/en-us/documentation/articles/documentdb-run-hadoop-with-hdinsight/) for more information.

##Contribute Code or Provide Feedback

If you would like to become an active contributor to this project please follow the instructions provided in [Azure Projects Contribution Guidelines](http://azure.github.io/guidelines.html).

If you encounter any bugs with the library please file an issue in the [Issues](https://github.com/Azure/azure-documentdb-hadoop/issues) section of the project.

##Learn More
* [DocumentDB with HDInsight Tutorial](http://azure.microsoft.com/en-us/documentation/articles/documentdb-run-hadoop-with-hdinsight/)
* [Official Hadoop Documentation](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html)
* [Azure Developer Center](http://azure.microsoft.com/en-us/develop/java/)
* [Azure DocumentDB Service](http://azure.microsoft.com/en-us/documentation/services/documentdb/)
* [Azure DocumentDB Team Blog](http://blogs.msdn.com/b/documentdb/)
