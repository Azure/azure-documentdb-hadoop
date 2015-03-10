//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
package com.microsoft.azure.documentdb.hadoop;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableSet;

/**
 * 
 * Provides the configuration properties needed for running a hadoop job on documentdb.
 *
 */
public class ConfigurationUtil {
    /**
     * The database Id used in the Map Reduce job.
     */
    public static final String DB_NAME = "DocumentDB.db";
    
    /**
     * Comma separated input collections Ids used in the map reduce job.
     */
    public static final String INPUT_COLLECTION_NAMES = "DocumentDB.inputCollections";
    
    /**
     * Comma separated outp collections Ids used in the map reduce job.
     */
    public static final String OUTPUT_COLLECTION_NAMES = "DocumentDB.outputCollections";
    
    /**
     * The link for the documentdb endpoint
     */
    public static final String DB_HOST = "DocumentDB.endpoint";
    
    /**
     * The masterkey used for the documentdb account.
     */
    public static final String DB_KEY = "DocumentDB.key";
    
    /**
     * the documentdb query pushed down to the input collections when reading.
     */
    public static final String QUERY = "DocumentDB.query";
    
    /**
     * Comma separated Property paths to range index in the output collections. 
     */
    public static final String OUTPUT_RANGE_INDEXED = "DocumentDB.rangeIndex";
    
    /**
     * An upsert option, true by default. This can be disabled by setting it to "false"
     */
    public static final String UPSERT = "DocumentDB.upsert";

    /**
     * Gets the DocumentDB.db from the Configuration object.
     * @param conf job configuration object
     * @return database Id
     */
    public final static String getDBName(Configuration conf) {
        return conf.get(DB_NAME);
    }
    
    /**
     * A set of all the configuration properties of the connector.
     */
    private static final Set<String> ALL_PROPERTIES = ImmutableSet.of(DB_NAME,
            INPUT_COLLECTION_NAMES, OUTPUT_COLLECTION_NAMES, DB_HOST, DB_KEY,
            QUERY);
  
    /**
     * Gets the DocumentDB.inputCollections from the Configuration object.
     * @param conf job configuration object
     * @return Array of collection Ids
     */
    public final static String[] getInputCollectionNames(Configuration conf) {
        String[] collectionNames = conf.get(INPUT_COLLECTION_NAMES).split(",");
        return collectionNames;
    }

    /**
     * Gets the DocumentDB.outputCollections from the Configuration object.
     * @param conf job configuration object
     * @return Array of collection Ids
     */
    public final static String[] getOutputCollectionNames(Configuration conf) {
        String[] collectionNames = conf.get(OUTPUT_COLLECTION_NAMES).split(",");
        return collectionNames;
    }

    /**
     * Gets the DocumentDB.endpoint from the Configuration object.
     * @param conf job configuration object
     * @return The documentdb endpoint url
     */
    public final static String getDBEndpoint(Configuration conf) {
        return conf.get(DB_HOST);
    }

    /**
     * Gets the DocumentDB.key from the Configuration object.
     * @param conf job configuration object.
     * @return The masterkey for documentdb database account.
     */
    public final static String getDBKey(Configuration conf) {
        return conf.get(DB_KEY);
    }

    /**
     * Gets the DocumentDB.query from the Configuration object.
     * @param conf job configuration object
     * @return sql query used to read from input collections.
     */
    public final static String getQuery(Configuration conf) {
        return conf.get(QUERY);
    }
    
    /**
     * Gets the DocumentDB.rangeIndex from the Configuration object.
     * @param conf job configuration object
     * @return array of property paths to range index.
     */
    public final static String[] getRangeIndex(Configuration conf) {
        String rangeIndexed = conf.get(OUTPUT_RANGE_INDEXED);
        String[] propertyNames = null;
        if (rangeIndexed != null) {
            propertyNames = rangeIndexed.split(",");
        } else {
            propertyNames = new String[] {};
        }
        
        return propertyNames;
    }

    /**
     * Gets the DocumentDB.upsert from the Configuration object.
     * @param conf job configuration object
     * @return the value of upsert option
     */
    public final static boolean getUpsert(Configuration conf) {
        String upsert = conf.get(UPSERT);
        return (upsert != null && upsert.equalsIgnoreCase("false")) ? false : true;
    }
    
    /**
     * Copies the configuration properties for the connector to a map.
     * @param from Properties object to copy from.
     * @param to Target map to copy properties to.
     */
    public static void copyDocumentDBProperties(Properties from, Map<String, String> to) {
        for (String key : ALL_PROPERTIES) {
            String value = from.getProperty(key);
            if (value != null) {
                to.put(key, value);
            }
        }
    }
}
