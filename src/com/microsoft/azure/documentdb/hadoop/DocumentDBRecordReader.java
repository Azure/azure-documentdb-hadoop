//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
package com.microsoft.azure.documentdb.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.microsoft.azure.documentdb.Document;

/**
 * Reads documents from documendb using a DocumentDBIterable instance.
 */
public class DocumentDBRecordReader extends
        RecordReader<LongWritable, DocumentDBWritable> {

    private DocumentDBInputSplit split;
    private Iterator<Document> documentIterator;
    private long documentsProcessed;
    private DocumentDBWritable current;
    private static final Log LOG = LogFactory.getLog(DocumentDBWritable.class);
    
    public DocumentDBRecordReader(DocumentDBInputSplit split) throws IOException {
        this.split = split;
        this.current = new DocumentDBWritable();
        this.documentIterator = this.split.getDocumentIterator();
    }

    public void close() throws IOException {

    }

    public float getProgress() throws IOException {
        if(this.documentIterator == null) return 0f;
        boolean hasNext = false;
        BackoffExponentialRetryPolicy policy = new BackoffExponentialRetryPolicy();
        while(policy.shouldRetry()) {
            try {
                hasNext = this.documentIterator.hasNext();
                break;
            }
            catch(Exception e) {
                policy.errorOccured(e);
            }
        }
        
        return hasNext ? 0f : 1f;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {
        return new LongWritable();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DocumentDBWritable getCurrentValue() throws IOException,
            InterruptedException {
        return current;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        if(this.split == null) this.split = (DocumentDBInputSplit) split;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        
        BackoffExponentialRetryPolicy retryPolicy = new BackoffExponentialRetryPolicy();
        while(retryPolicy.shouldRetry()) {
            try {
                if (this.documentIterator == null || !this.documentIterator.hasNext()) {
                    LOG.info(String.format("processed %d documents of collection %s", this.documentsProcessed, this.split.getCollectionName()));
                    return false;
                }
                
                if(documentsProcessed % 100 == 0) {
                    LOG.info(String.format("processed %d documents of collection %s", this.documentsProcessed, this.split.getCollectionName()));
                }
                
                this.current.setDoc(this.documentIterator.next());
                this.documentsProcessed++;
                break;
            } catch(Exception e) {
                retryPolicy.errorOccured(e);
            }
        }
        
        return true;
    }
}
