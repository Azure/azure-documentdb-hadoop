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

    /**
     * @inheritDoc
     */
    @Override
    public void abortTask(final TaskAttemptContext taskContext) {
        LOG.info("Aborting task.");
    }

    /**
     * @inheritDoc
     */
    @Override
    public void commitTask(final TaskAttemptContext taskContext) {
        LOG.info("Committing task.");
    }

    /**
     * @inheritDoc
     */
    @Override
    public boolean needsTaskCommit(final TaskAttemptContext taskContext) {
        return true;
    }

    /**
     * @inheritDoc
     */
    @Override
    public void setupJob(final JobContext jobContext) {
        LOG.info("Setting up job.");
    }

    /**
     * @inheritDoc
     */
    @Override
    public void setupTask(final TaskAttemptContext taskContext) {
        LOG.info("Setting up task.");
    }

}