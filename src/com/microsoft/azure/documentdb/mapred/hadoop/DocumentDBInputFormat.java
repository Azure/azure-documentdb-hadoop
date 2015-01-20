//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
package com.microsoft.azure.documentdb.mapred.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hive.com.esotericsoftware.minlog.Log;

import com.microsoft.azure.documentdb.hadoop.ConfigurationUtil;
import com.microsoft.azure.documentdb.hadoop.DocumentDBInputSplit;
import com.microsoft.azure.documentdb.hadoop.DocumentDBWritable;

public class DocumentDBInputFormat implements InputFormat<LongWritable, DocumentDBWritable> {

    public RecordReader<LongWritable, DocumentDBWritable> getRecordReader(InputSplit split, JobConf conf,
            Reporter reporter) throws IOException {

        return new DocumentDBRecordReader((WrapperSplit) split);
    }

    public InputSplit[] getSplits(JobConf conf, int numberOfSplits) throws IOException {
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

        List<org.apache.hadoop.mapreduce.InputSplit> innerSplits = DocumentDBInputSplit.getSplits(conf, endpoint, key,
                dbName, collectionNames, query);
        Path[] tablePaths = FileInputFormat.getInputPaths(conf);
        ArrayList<InputSplit> ret = new ArrayList<InputSplit>();

        for (org.apache.hadoop.mapreduce.InputSplit split : innerSplits) {
            ret.add(new WrapperSplit((DocumentDBInputSplit) split, tablePaths[0], conf));
        }

        return ret.toArray(new InputSplit[0]);
    }
}
