//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
package com.microsoft.azure.documentdb.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class DocumentDBOutputCommitter extends OutputCommitter {

    private static final Log LOG = LogFactory.getLog(DocumentDBWritable.class);

    @Override
    public void abortTask(final TaskAttemptContext taskContext) {
        LOG.info("Aborting task.");
    }

    @Override
    public void commitTask(final TaskAttemptContext taskContext) {
        LOG.info("Committing task.");
    }

    @Override
    public boolean needsTaskCommit(final TaskAttemptContext taskContext) {
        return true;
    }

    @Override
    public void setupJob(final JobContext jobContext) {
        LOG.info("Setting up job.");
    }

    @Override
    public void setupTask(final TaskAttemptContext taskContext) {
        LOG.info("Setting up task.");
    }

}