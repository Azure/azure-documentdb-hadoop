//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
package com.microsoft.azure.documentdb.mapred.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import com.microsoft.azure.documentdb.hadoop.ConfigurationUtil;
import com.microsoft.azure.documentdb.hadoop.DocumentDBWritable;

/**
 * An output format that is used to write data to documentdb.
 */
public class DocumentDBOutputFormat implements OutputFormat<Writable, DocumentDBWritable> {

    /**
     * Validates the required properties needed to write to documentdb.
     */
    public void checkOutputSpecs(FileSystem fs, JobConf conf) throws IOException {
        final String endpoint = ConfigurationUtil.getDBEndpoint(conf);
        final String key = ConfigurationUtil.getDBKey(conf);
        final String dbName = ConfigurationUtil.getDBName(conf);
        final String[] collectionNames = ConfigurationUtil.getOutputCollectionNames(conf);

        if (endpoint == null)
            throw new IOException("DB_HOST must be set for the jobconf");
        if (key == null)
            throw new IOException("DB_KEY must be set for the jobconf");
        if (dbName == null)
            throw new IOException("DB_NAME must be set for the jobconf");
        if (collectionNames == null || collectionNames.length == 0)
            throw new IOException("OUTPUT_COLLECTION_NAME must be set for the jobconf as comma separated names");
    }

    /**
     * Creates an instance of DocumentDBRecordWriter.
     */
    public RecordWriter<Writable, DocumentDBWritable> getRecordWriter(FileSystem fs, JobConf conf, String arg2,
            Progressable arg3) throws IOException {

        return new DocumentDBRecordWriter(conf, ConfigurationUtil.getDBEndpoint(conf),
                ConfigurationUtil.getDBKey(conf), ConfigurationUtil.getDBName(conf),
                ConfigurationUtil.getOutputCollectionNames(conf),
                ConfigurationUtil.getOutputStringPrecision(conf),
                ConfigurationUtil.getUpsert(conf), ConfigurationUtil.getOutputCollectionsOffer(conf));
    }
}
