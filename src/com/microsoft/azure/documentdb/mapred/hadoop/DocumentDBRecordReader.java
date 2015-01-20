//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
package com.microsoft.azure.documentdb.mapred.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.RecordReader;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.hadoop.DocumentDBConnectorUtil;
import com.microsoft.azure.documentdb.hadoop.DocumentDBWritable;

public class DocumentDBRecordReader implements RecordReader<LongWritable, DocumentDBWritable> {

    private long pos;
    private long documentsProcessed;
    private Iterator<Document> documentIterator;
    private WrapperSplit split;
    private static final Log LOG = LogFactory.getLog(DocumentDBWritable.class);


    /**
    * A record reader using the old mapred.* API that reads entities
    * from DocumentDB.
    */
    public DocumentDBRecordReader(WrapperSplit split) throws IOException {
        this.split = split;
        this.documentIterator = this.split.getWrappedSplit().getDocumentIterator();
    }

    public void close() throws IOException {

    }

    public LongWritable createKey() {
        return new LongWritable();
    }

    public DocumentDBWritable createValue() {
        return new DocumentDBWritable();
    }

    public long getPos() throws IOException {
        return this.pos;
    }

    public float getProgress() throws IOException {
        if (this.documentIterator == null)
            return 0f;
        return this.documentIterator.hasNext() ? 0f : 1f;
    }

    /**
     * Gets the next writable from DocumentDb
     */
    public boolean next(LongWritable key, DocumentDBWritable value) throws IOException {
        if (this.documentIterator == null || !this.documentIterator.hasNext()) {
            LOG.info(String.format("processed %d documents of collection %s", this.documentsProcessed, this.split
                    .getWrappedSplit().getCollectionName()));
            return false;
        }

        if (documentsProcessed % 100 == 0) {
            LOG.info(String.format("processed %d documents of collection %s", this.documentsProcessed, this.split
                    .getWrappedSplit().getCollectionName()));
        }

        value.setDoc(this.documentIterator.next());
        documentsProcessed++;
        return true;
    }
}
