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
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.QueryIterable;
import com.microsoft.azure.documentdb.StoredProcedure;
import com.microsoft.azure.documentdb.hadoop.DocumentDBWritable;
import com.microsoft.azure.documentdb.hadoop.DocumentDBConnectorUtil;

public class DocumentDBRecordWriter implements RecordWriter<Writable, DocumentDBWritable> {
    private static final Log LOG = LogFactory.getLog(DocumentDBWritable.class);
    private static int MAX_DOC_SIZE = 25;
    private DocumentClient client;
    private boolean enableUpsert;
    private DocumentCollection[] collections;
    private StoredProcedure[] sprocs;
    private int documentsProcessed = 0;
    private StoredProcedure sproc;
    private List<Document> cachedDocs;
    private int currentStoredProcedureIndex = 0;
    
    public DocumentDBRecordWriter(JobConf conf, String host, String key, String dbName, String[] collNames,
            String[] rangeIndexes, boolean upsert) throws IOException {
        DocumentClient client;
        try {
            client = new DocumentClient(host, key, ConnectionPolicy.GetDefault(), ConsistencyLevel.Session);

            QueryIterable<Database> dbIterable = client.queryDatabases(
                    String.format("select * from root r where r.id =\"%s\"", dbName), null).getQueryIterable();
            List<Database> databases = dbIterable.toList();
            if (databases.size() != 1) {
                throw new IOException(String.format("Database %s doesn't exist", dbName));
            }

            Database db = databases.get(0);
            this.client = client;
            this.collections = new DocumentCollection[collNames.length];
            this.sprocs = new StoredProcedure[collNames.length];
            for (int i = 0; i < collNames.length; i++) {
                this.collections[i] =  DocumentDBConnectorUtil.createOutputCollection(client, db.getSelfLink(), collNames[i],
                        rangeIndexes);
                this.sprocs[i] = DocumentDBConnectorUtil.CreateBulkImportStoredProcedure(client, this.collections[i].getSelfLink());
            }
            
            this.enableUpsert = upsert;
            this.cachedDocs = new LinkedList<Document>();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    public void close(Reporter reporter) throws IOException {
        if (this.cachedDocs.size() > 0) {
            this.writeCurrentBatch();
        }
    }

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
