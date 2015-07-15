//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
package com.microsoft.azure.documentdb.mapred.hadoop;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DataType;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.QueryIterable;
import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import com.microsoft.azure.documentdb.StoredProcedure;
import com.microsoft.azure.documentdb.hadoop.BackoffExponentialRetryPolicy;
import com.microsoft.azure.documentdb.hadoop.DocumentDBWritable;
import com.microsoft.azure.documentdb.hadoop.DocumentDBConnectorUtil;

/**
 * Writes data to DocumentDB in document batches using a stored procedure.
 */
public class DocumentDBRecordWriter implements RecordWriter<Writable, DocumentDBWritable> {
    private static final Log LOG = LogFactory.getLog(DocumentDBWritable.class);
    private static int MAX_DOC_SIZE = 50;
    private DocumentClient client;
    private boolean enableUpsert;
    private DocumentCollection[] collections;
    private StoredProcedure[] sprocs;
    private int documentsProcessed = 0;
    private StoredProcedure sproc;
    private List<Document> cachedDocs;
    private int currentStoredProcedureIndex = 0;
    
    public DocumentDBRecordWriter(JobConf conf, String host, String key, String dbName, String[] collNames,
            int outputStringPrecision, boolean upsert, String offerType) throws IOException {
        DocumentClient client;
        try {
            ConnectionPolicy policy = ConnectionPolicy.GetDefault();
            policy.setUserAgentSuffix(DocumentDBConnectorUtil.UserAgentSuffix);
            client = new DocumentClient(host, key, policy, ConsistencyLevel.Session);
            Database db = DocumentDBConnectorUtil.GetDatabase(client, dbName);
            this.client = client;
            this.collections = new DocumentCollection[collNames.length];
            this.sprocs = new StoredProcedure[collNames.length];
            for (int i = 0; i < collNames.length; i++) {
                this.collections[i] =  DocumentDBConnectorUtil.getOrCreateOutputCollection(client, db.getSelfLink(), collNames[i],
                        outputStringPrecision, offerType);
                this.sprocs[i] = DocumentDBConnectorUtil.CreateBulkImportStoredProcedure(client, this.collections[i].getSelfLink());
            }
            
            this.enableUpsert = upsert;
            this.cachedDocs = new LinkedList<Document>();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    /**
     * Writes the last batch of documents that are being cached.
     */
    public void close(Reporter reporter) throws IOException {
        if (this.cachedDocs.size() > 0) {
            this.writeCurrentBatch();
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
    
    private void writeCurrentBatch() {
        DocumentDBConnectorUtil.executeWriteStoredProcedure(this.client, 
                this.collections[this.currentStoredProcedureIndex].getSelfLink(),
                this.sprocs[this.currentStoredProcedureIndex], this.cachedDocs,
                this.enableUpsert);
        this.cachedDocs.clear();
        
        // Do a round robin on the collections and execute the stored procedure once per each.
        this.currentStoredProcedureIndex = (this.currentStoredProcedureIndex + 1) % this.sprocs.length;
    }
}
