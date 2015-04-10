//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

package com.microsoft.azure.documentdb.pig;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreMetadata;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.json.JSONException;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.hadoop.ConfigurationUtil;
import com.microsoft.azure.documentdb.hadoop.DocumentDBConnectorUtil;
import com.microsoft.azure.documentdb.hadoop.DocumentDBOutputFormat;
import com.microsoft.azure.documentdb.hadoop.DocumentDBRecordWriter;
import com.microsoft.azure.documentdb.hadoop.DocumentDBWritable;

/**
 * An implementation of Pig StoreFunc for documentdb.
 */
public class DocumentDBStorage extends StoreFunc implements StoreMetadata {
    private static final String PIG_STORAGE_USERAGENT = " PigConnectorStorage/1.0.0";
    private String masterkey = null;
    private DocumentDBRecordWriter writer = null;
    protected ResourceSchema schema = null;
    private String dbName;
    private String outputCollections;
    private String rangeIndexed;
    private String upsert;
    private String offerType;
    private String udfContextSignature = null;
    
    // Pig specific settings
    static final String PIG_OUTPUT_SCHEMA = "documentdb.pig.output.schema";
    static final String PIG_OUTPUT_SCHEMA_UDF_CONTEXT = "documentdb.pig.output.schema.udf_context";
    
    private static final Log LOG = LogFactory.getLog(DocumentDBStorage.class);

    public DocumentDBStorage(String masterkey, String dbName, String outputCollections){
        this(masterkey, dbName, outputCollections, null, null, null);
    }
    
    public DocumentDBStorage(String masterkey, String dbName, String outputCollections, String rangeindexed, String upsert, String offerType) {
        this.masterkey = masterkey;
        this.dbName = dbName;
        this.outputCollections =  outputCollections;
        this.upsert = upsert;
        this.rangeIndexed = rangeindexed;
        this.offerType = offerType;
        
        //Set the userAgent to pig storage
        if (!DocumentDBConnectorUtil.UserAgentSuffix.contains(DocumentDBStorage.PIG_STORAGE_USERAGENT)) {
            DocumentDBConnectorUtil.UserAgentSuffix += DocumentDBStorage.PIG_STORAGE_USERAGENT;
        }
    }
    
    /**
     * Returns an instance of DocumentDBOutputFormat.
     */
    public OutputFormat getOutputFormat() throws IOException {
        return new DocumentDBOutputFormat();
    }

    /**
     * Sets the DocumentDB connector output configuration properties.          
     */
    public void setStoreLocation(final String location, final Job job) throws IOException {
        Configuration conf = job.getConfiguration();

        conf.set(ConfigurationUtil.DB_HOST, location);
        conf.set(ConfigurationUtil.DB_KEY, this.masterkey);
        conf.set(ConfigurationUtil.DB_NAME, this.dbName);
        conf.set(ConfigurationUtil.OUTPUT_COLLECTION_NAMES, this.outputCollections);
        if (this.upsert != null) {
            conf.set(ConfigurationUtil.UPSERT, this.upsert);
        }
        
        if (this.rangeIndexed != null) {
            conf.set(ConfigurationUtil.OUTPUT_RANGE_INDEXED, this.rangeIndexed);
        }
        
        if(this.offerType != null) {
            conf.set(ConfigurationUtil.OUTPUT_COLLECTIONS_OFFER, this.offerType);
        }
        
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
    }

    /**
     * {@inheritDoc}
     */
    public void checkSchema(final ResourceSchema schema) throws IOException {
        this.schema = schema;
        
        final Properties properties =
                UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[]{udfContextSignature});
        properties.setProperty(PIG_OUTPUT_SCHEMA_UDF_CONTEXT, schema.toString());
    }
    
    /**
     * {@inheritDoc}
     */
    public void storeStatistics(ResourceStatistics stats, String location,
            Job job) throws IOException {
    }

    /**
     * {@inheritDoc}
     */
    public void storeSchema(ResourceSchema schema, String location, Job job)
            throws IOException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        this.writer = (DocumentDBRecordWriter) writer;
        
        // Parse the schema from the string stored in the properties object.
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p = udfc.getUDFProperties(this.getClass(), new String[]{udfContextSignature});

        String strSchema = p.getProperty(PIG_OUTPUT_SCHEMA_UDF_CONTEXT);
        if (strSchema == null) {
            throw new IOException("Could not find schema in UDF context");
        }

        try {
            // Parse the schema from the string stored in the properties object.
            this.schema = new ResourceSchema(SchemaHelper.getSchemaFromString(strSchema));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

    }

    /**
     * Sends a document to DocumentDBRecordWrites by converting a pig tuple to DocumentDBWritable.
     */
    @Override
    public void putNext(Tuple t) throws IOException {
        DocumentDBWritable writable = new DocumentDBWritable();
        Document doc = new Document();
        
        if (this.schema == null) {
            LOG.debug(String.format("schema is null!"));
            // dynamic schema: we will output a tuple with one element, which is
            // map storing the key-value of JSON.
            
            List<Object> list = t.getAll();
            
            int count = 0;
            for (Object object : list) {
                if (object instanceof Map) {
                    Map mapObject = (Map)object;
                    for (Object key: mapObject.keySet()) {
                        Object value = mapObject.get(key);
                        
                        doc.set(key.toString(), value);
                    }
                  
                } else if (object instanceof String) {
                    doc.set(String.format("field%s",count) , object);
                    count++;
                } else  {
                    LOG.error(String.format("Object type: %s", object.getClass().toString()));
                }
            }
            
        } else {
            ResourceFieldSchema[] fields = this.schema.getFields();
            for (int i = 0; i < fields.length; i++) {
                ResourceFieldSchema field = fields[i];
                doc.set(field.getName(), t.get(i));
            }
        }
        
        writable.setDoc(doc);
        this.writer.write(null, writable);
    }

    /**
     * {@inheritDoc}
     */
    public void setStoreFuncUDFContextSignature(final String signature) {
        udfContextSignature = signature;
    }
}
