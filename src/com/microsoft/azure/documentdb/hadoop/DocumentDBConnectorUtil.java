package com.microsoft.azure.documentdb.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.jetty.HttpStatus;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.IndexType;
import com.microsoft.azure.documentdb.IndexingPath;
import com.microsoft.azure.documentdb.IndexingPolicy;
import com.microsoft.azure.documentdb.QueryIterable;
import com.microsoft.azure.documentdb.StoredProcedure;

public class DocumentDBConnectorUtil {
    private static final Log LOG = LogFactory.getLog(DocumentDBWritable.class);
    private final static int REQUEST_RATE_TOO_LARGE = 429;
    private final static int MAX_SCRIPT_DOCS = 25;
    private final static int MAX_SCRIPT_SIZE = 50000;
    private final static String BULK_IMPORT_ID = "HadoopBulkImportSprocV1";
    private final static String BULK_IMPORT_PATH = "/BulkImportScript.js";
    
    /**
     * Gets an output collection with the passed name ( if the collection already exists return it, otherwise create new one
     * @param client The DocumentClient instance.
     * @param databaseSelfLink the self link of the passed database.
     * @param collectionName The id of the output collection.
     * @param rangeIndexes An optional parameter that contain index paths for range indexes and it will be used to create an indexing policy.
     */
    public static DocumentCollection createOutputCollection(DocumentClient client, String databaseSelfLink,
            String collectionName, String[] rangeIndexes) throws DocumentClientException {
        QueryIterable<DocumentCollection> collIterable = client.queryCollections(databaseSelfLink,
                "select * from root r where r.id = \"" + collectionName + "\"", null).getQueryIterable();
        List<DocumentCollection> collections = collIterable.toList();
        if (collections.size() != 1) {
            DocumentCollection outputColl = new DocumentCollection("{ 'id':'" + collectionName + "' }");
            if (rangeIndexes.length > 0) {
                IndexingPolicy policy = new IndexingPolicy();
                ArrayList<IndexingPath> indexingPaths = new ArrayList<IndexingPath>(rangeIndexes.length);
                for (int i = 0; i < rangeIndexes.length; i++) {
                    IndexingPath path = new IndexingPath();
                    path.setIndexType(IndexType.Range);
                    path.setPath(rangeIndexes[i]);
                    indexingPaths.add(path);
                }

                IndexingPath defaultPath = new IndexingPath();
                defaultPath.setPath("/");
                indexingPaths.add(defaultPath);
                policy.getIncludedPaths().addAll(indexingPaths);
                outputColl.setIndexingPolicy(policy);
            }

            return client.createCollection(databaseSelfLink, outputColl, null).getResource();
        } else {
            return collections.get(0);
        }
    }

    /**
     * Gets the bulk import stored procedure that will be used for writing documents ( if the sproc already exists, use it, otherwise create a new one.
     * @param client the DocumentClient instance for DocumentDB.
     * @param collectionLink the self-link of the collection to write to.
     * @return StoredProcedure instance that will be used for writing
     */
    public static StoredProcedure CreateBulkImportStoredProcedure(DocumentClient client, String collectionLink)
            throws DocumentClientException {
        String query = String.format("select * from root r where r.id = '%s'", BULK_IMPORT_ID);
        List<StoredProcedure> sprocs = client.queryStoredProcedures(collectionLink, query, null).getQueryIterable().toList();
        if(sprocs.size() > 0) {
            return sprocs.get(0);
        }
        
        StoredProcedure sproc = new StoredProcedure();
        sproc.setId(BULK_IMPORT_ID);
        String sprocBody = getBulkImportBody(client);
        sproc.setBody(sprocBody);
        return client.createStoredProcedure(collectionLink, sproc, null).getResource();
    }

    /**
     * Executes the bulk import stored procedure for a list of documents.
     * The execution takes into consideration throttling and blacklisting of the stored procedure.
     * @param client The DocumentClient instance for DocumentDB
     * @param collectionSelfLink the self-link for the collection to write to.
     * @param sproc The stored procedure to execute
     * @param allDocs The list of documents to write
     * @param upsert  Specifies whether to replace the document if exists or not. By default it's true.
     */
    public static void executeWriteStoredProcedure(final DocumentClient client, String collectionSelfLink, final StoredProcedure sproc,
            List<Document> allDocs, final boolean upsert) {
        try {
            int currentCount = 0;
            int docCount = Math.min(allDocs.size(), MAX_SCRIPT_DOCS);
            
            while (currentCount < docCount)
            {
                String []jsonArrayString = CreateBulkInsertScriptArguments(allDocs, currentCount, docCount, MAX_SCRIPT_SIZE);
                String response = client.executeStoredProcedure(sproc.getSelfLink(), new Object[] {jsonArrayString, upsert })
                        .getResponseAsString();
                int createdCount = Integer.parseInt(response);
                
                currentCount += createdCount;
            }
        } catch (DocumentClientException e) {
            if (e.getStatusCode() == REQUEST_RATE_TOO_LARGE) {
                LOG.error("Throttled, retrying after:"+e.getRetryAfterInMilliseconds());
                
               try {
                   Thread.sleep(e.getRetryAfterInMilliseconds());
               } catch (InterruptedException e1) {
                   throw new IllegalStateException(e1);
               }
               
               executeWriteStoredProcedure(client, collectionSelfLink, sproc, allDocs, upsert);
            } else if (e.getStatusCode() == HttpStatus.ORDINAL_403_Forbidden) {
                // Recreate the stored procedure if it gets blacklisted ( should never happen )
                recreateStoredProcedure(client, collectionSelfLink, sproc);
                executeWriteStoredProcedure(client, collectionSelfLink, sproc, allDocs, upsert);
            } else {
                e.printStackTrace();
                throw new IllegalStateException(e);
            }
        }
    }
    
    /**
     * Used to delete the stored procedure and recreate it if it gets blacklisted
     */
    private static void recreateStoredProcedure(DocumentClient client, String collectionSelfLink, StoredProcedure sproc) {
        try {
            LOG.error("sproc got recreated after blacklisting");
            client.deleteStoredProcedure(sproc.getSelfLink(), null);
            StoredProcedure createdSproc = CreateBulkImportStoredProcedure(client, collectionSelfLink);
            
            // Workaround to change the self-link of the already created stored procedure.
            sproc.set("_self", createdSproc.getSelfLink());
        } catch (DocumentClientException e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
        
    }

    /**
     * 
     * @param docs The list of documents to be created 
     * @param currentIndex the current index in the list of docs to start with.
     * @param maxCount the max count to be created by the sproc.
     * @param maxScriptSize the max size of the sproc that is used to avoid exceeding the max request size.
     * @return
     */
    private static String[] CreateBulkInsertScriptArguments(List<Document> docs, int currentIndex, int maxCount, int maxScriptSize)
    {
        if (currentIndex >= maxCount) return new String[]{};
        
        ArrayList<String> jsonDocumentList = new ArrayList<String>();
        String stringifiedDoc = docs.get(0).toString();
        jsonDocumentList.add(stringifiedDoc);
        int scriptCapacityRemaining = maxScriptSize - stringifiedDoc.length();

        int i = 1;
        while (scriptCapacityRemaining > 0 && (currentIndex + i) < maxCount)
        {
            stringifiedDoc = docs.get(currentIndex + i).toString();
            jsonDocumentList.add(stringifiedDoc);
            scriptCapacityRemaining-= stringifiedDoc.length();
            i++;
        }

        String[] jsonDocumentArray = new String[jsonDocumentList.size()];
        jsonDocumentList.toArray(jsonDocumentArray);
        return jsonDocumentArray;
    }
    
    /**
     * Reads the bulk import script body from the file.
     * @param client the DocumentClient instance.
     * @return a string that contains the stored procedure body.
     */
    private static String getBulkImportBody(DocumentClient client) {
        try {
            InputStream stream = DocumentDBConnectorUtil.class.getResourceAsStream(BULK_IMPORT_PATH);
            List<String> scriptLines = IOUtils.readLines(stream);
            StringBuilder scriptBody = new StringBuilder();
            for (Iterator<String> iterator = scriptLines.iterator(); iterator.hasNext();) {
                String line = (String) iterator.next();
                scriptBody.append(line + "\n");
            }

            return scriptBody.toString();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * If no id is provided, replace it with an auto generated guid id.
     * @param doc The document to be checked for id.
     */
    public static void addIdIfMissing(Document doc) {
        if (doc.getId() == null) {
            doc.setId(UUID.randomUUID().toString());
        }
    }
}
