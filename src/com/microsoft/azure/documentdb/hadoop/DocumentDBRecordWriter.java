//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
package com.microsoft.azure.documentdb.hadoop;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.StoredProcedure;

/**
 * Writes data to DocumentDB in document batches using a stored procedure.
 */
public class DocumentDBRecordWriter extends RecordWriter<Writable, DocumentDBWritable> {
    private static final Log LOG = LogFactory.getLog(DocumentDBWritable.class);
    private static int MAX_DOC_SIZE = 50;
    private DocumentClient client;
    private DocumentCollection[] collections;
    private StoredProcedure[] sprocs;
    private boolean enableUpsert;
    private int documentsProcessed = 0;
    private List<Document> cachedDocs;
    private int currentStoredProcedureIndex = 0;
    
    public DocumentDBRecordWriter(Configuration conf, String host, String key, String dbName, String[] collNames,
            int outputStringPrecision, boolean upsert, String offerType) throws IOException {
        try {
            ConnectionPolicy policy = ConnectionPolicy.GetDefault();
            policy.setUserAgentSuffix(DocumentDBConnectorUtil.UserAgentSuffix);
            DocumentClient client = new DocumentClient(host, key, policy,
                    ConsistencyLevel.Session);

            Database db = DocumentDBConnectorUtil.GetDatabase(client, dbName);
            this.collections = new DocumentCollection[collNames.length];
            this.sprocs = new StoredProcedure[collNames.length];
            for (int i = 0; i < collNames.length; i++) {
                this.collections[i] =  DocumentDBConnectorUtil.getOrCreateOutputCollection(client, db.getSelfLink(), collNames[i],
                        outputStringPrecision, offerType);
                this.sprocs[i] = DocumentDBConnectorUtil.CreateBulkImportStoredProcedure(client, this.collections[i].getSelfLink());
            }
            
            this.client = client;
            this.enableUpsert = upsert;
            this.cachedDocs = new LinkedList<Document>();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    /**
     * Writes data to DocumentDB if the cached documents reach the maximum cache size.
     */
    public void write(Writable key, DocumentDBWritable value) throws IOException {
        Document doc = value.getDoc();
        DocumentDBConnectorUtil.addIdIfMissing(doc);
        this.cachedDocs.add(doc);
        this.documentsProcessed++;
        if (documentsProcessed % MAX_DOC_SIZE == 0) {
            this.writeCurrentBatch();
            LOG.info(String.format("wrote %d documents", this.documentsProcessed));
        }
    }

    /**
     * Writes the last batch of documents that are being cached.
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (this.cachedDocs.size() > 0) {
            this.writeCurrentBatch();
        }
    }
    
    private void writeCurrentBatch() {
        // Writing to output collections is round robin for each batch.
        DocumentDBConnectorUtil.executeWriteStoredProcedure(this.client, 
                this.collections[this.currentStoredProcedureIndex].getSelfLink(),
                this.sprocs[this.currentStoredProcedureIndex], this.cachedDocs,
                this.enableUpsert);
        this.cachedDocs.clear();
        
        // Do a round robin on the collections and execute the stored procedure once per each.
        this.currentStoredProcedureIndex = (this.currentStoredProcedureIndex + 1) % this.sprocs.length;
    }

}
