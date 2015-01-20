//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
package com.microsoft.azure.documentdb.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An input format that can read data from Azure DocumentDB. It sends one Document 
 * at a time to the mapper.
 */
public class DocumentDBInputFormat extends InputFormat<LongWritable, DocumentDBWritable> {

    private static final Log LOG = LogFactory.getLog(DocumentDBWritable.class);
    @Override
    public RecordReader<LongWritable, DocumentDBWritable> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
        return new DocumentDBRecordReader((DocumentDBInputSplit) split);
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        final String endpoint = ConfigurationUtil.getDBEndpoint(conf);
        final String key = ConfigurationUtil.getDBKey(conf);
        final String dbName = ConfigurationUtil.getDBName(conf);
        final String[] collectionNames = ConfigurationUtil.getInputCollectionNames(conf);
        final String query = ConfigurationUtil.getQuery(conf);

        if (endpoint == null)
            throw new IOException("DB_HOST must be set for the jobconf");
        if (key == null)
            throw new IOException("DB_KEY must be set for the jobconf");
        if (dbName == null)
            throw new IOException("DB_NAME must be set for the jobconf");
        if (collectionNames.length < 1)
            throw new IOException("INPUT_COLLECTION_NAMES must be set for the jobconf as comma separated names");
        return DocumentDBInputSplit.getSplits(conf, endpoint, key, dbName, collectionNames, query);
    }
}