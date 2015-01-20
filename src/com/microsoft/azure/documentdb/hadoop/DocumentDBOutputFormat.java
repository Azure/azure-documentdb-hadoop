//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
package com.microsoft.azure.documentdb.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * An output format that is used to write data to documentdb.
 *
 */
public class DocumentDBOutputFormat extends OutputFormat<Writable, DocumentDBWritable> {
    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
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
            throw new IOException("OUTPUT_COLLECTION_NAMES must be set for the jobconf as comma separated names");
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new DocumentDBOutputCommitter();
    }

    @Override
    public RecordWriter<Writable, DocumentDBWritable> getRecordWriter(TaskAttemptContext context) throws IOException,
            InterruptedException {
        Configuration conf = context.getConfiguration();
        return new DocumentDBRecordWriter(conf, ConfigurationUtil.getDBEndpoint(conf),
                ConfigurationUtil.getDBKey(conf), ConfigurationUtil.getDBName(conf),
                ConfigurationUtil.getOutputCollectionNames(conf), ConfigurationUtil.getRangeIndex(conf),
                ConfigurationUtil.getUpsert(conf));
    }
}
