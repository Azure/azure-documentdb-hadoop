//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

package com.microsoft.azure.documentdb.pig;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.hadoop.ConfigurationUtil;
import com.microsoft.azure.documentdb.hadoop.DocumentDBConnectorUtil;
import com.microsoft.azure.documentdb.hadoop.DocumentDBInputFormat;
import com.microsoft.azure.documentdb.hadoop.DocumentDBWritable;

/**
 * A Pig data loader from DocumentDB.
 */
public class DocumentDBLoader extends LoadFunc{
    private static final String PIG_LOADER_USERAGENT = " PigConnectorLoader/1.0.0";
    private String masterkey = null;
    private RecordReader reader = null;
    private ResourceFieldSchema[] fields;
    private ResourceSchema schema = null;
    private String dbName;
    private String inputCollections;
    private String query;
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    Log log = LogFactory.getLog(DocumentDBLoader.class);

    public DocumentDBLoader(String masterkey, String dbName, String inputCollections){
        this(masterkey, dbName, inputCollections, null);
    }
    
    public DocumentDBLoader(String masterkey, String dbName, String inputCollections, String query) {
        this.masterkey = masterkey;
        this.fields = null;
        this.dbName = dbName;
        // Comma separated collection names
        this.inputCollections = inputCollections; 
        this.query = query;
        
        //Set the userAgent to pig loader
        if (!DocumentDBConnectorUtil.UserAgentSuffix.contains(DocumentDBLoader.PIG_LOADER_USERAGENT)) {
            DocumentDBConnectorUtil.UserAgentSuffix += DocumentDBLoader.PIG_LOADER_USERAGENT;
        }
    }

    /**
     * Returns an instance of DocumentDBInputFormat
     */
    @Override
    public InputFormat getInputFormat() throws IOException {
        log.debug("getting input format");
        return new DocumentDBInputFormat();
    }

    private Tuple translate(DocumentDBWritable value) throws IOException {
        Tuple t = null;
        Document doc = value.getDoc();
        
        if (this.fields == null) {
            // dynamic schema: we will output a tuple with one element, which is
            // map storing the key-value of JSON.
            
            HashMap<String, Object> properties = doc.getHashMap();
            
            t = tupleFactory.newTuple(1);
            t.set(0, convertToPigType(properties));
        } else {
            // TODO: when scheme is specified.
            t = tupleFactory.newTuple(this.fields.length);
            for (int i = 0; i < this.fields.length; i++) {
                String fieldTemp = this.fields[i].getName();
                t.set(i, convertToPigType(doc.getObject(fieldTemp)));
            }
               
        }
        
        return t;
    }

    private Object convertToPigType(Object o) throws ExecException{
        if (o == null || o.equals(null)) {
            return null;
        } else if (o instanceof Number || o instanceof String) {
            return o;
        } else if (o instanceof ArrayList) {
            ArrayList list = (ArrayList) o;
            Tuple t = tupleFactory.newTuple(list.size());
            for (int i = 0; i < list.size(); i++) {
                t.set(i, convertToPigType(list.get(i)));
            }
            
            return t;
        } else if (o instanceof Map) {
            // TODO make this more efficient for lazy objects?
            Map<String, Object> fieldsMap = (Map<String, Object>) o;
            HashMap<String, Object> pigMap = new HashMap<String, Object>(fieldsMap.size());
            for (Map.Entry<String, Object> field : fieldsMap.entrySet()) {
                pigMap.put(field.getKey(), convertToPigType(field.getValue()));
            }
            
            return pigMap;
        } else {
            return o;
        }
    }
    
    /**
     * Converts a DocumentDBWritable to a pig Tuple.
     */
    @Override
    public Tuple getNext() throws IOException {
        LongWritable key = null;
        DocumentDBWritable value = null;

        try {
            if (!reader.nextKeyValue()) return null;
            key = (LongWritable) reader.getCurrentKey();
            value = (DocumentDBWritable) reader.getCurrentValue();

        } catch (InterruptedException e) {
            throw new IOException("Error reading in key/value", e);
        }

        if (key == null || value == null) {
            return null;
        }

        return this.translate(value);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
            throws IOException {
        this.reader = reader;
    }

    /**
     * Sets the DocumentDB connector input configuration properties.
     */
    @Override
    public void setLocation(String location, Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        
        conf.set(ConfigurationUtil.DB_HOST, location);
        conf.set(ConfigurationUtil.DB_KEY, this.masterkey);
        conf.set(ConfigurationUtil.DB_NAME, this.dbName);
        conf.set(ConfigurationUtil.INPUT_COLLECTION_NAMES, this.inputCollections);
        if (this.query != null) {
            conf.set(ConfigurationUtil.QUERY, this.query);
        }
        
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String relativeToAbsolutePath(final String location, final Path currentDir) {
        return location;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setUDFContextSignature(String signature) {
        
    }

}
