//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
package com.microsoft.azure.documentdb.hadoop;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableSet;

public class ConfigurationUtil {
    public static final String DB_NAME = "DocumentDB.db";
    public static final String INPUT_COLLECTION_NAMES = "DocumentDB.inputCollections";
    public static final String OUTPUT_COLLECTION_NAMES = "DocumentDB.outputCollections";
    public static final String DB_HOST = "DocumentDB.endpoint";
    public static final String DB_KEY = "DocumentDB.key";
    public static final String QUERY = "DocumentDB.query";
    public static final String OUTPUT_RANGE_INDEXED = "DocumentDB.rangeIndex";
    public static final String UPSERT = "DocumentDB.upsert";
    
    public static final Set<String> ALL_PROPERTIES = ImmutableSet.of(DB_NAME,
            INPUT_COLLECTION_NAMES, OUTPUT_COLLECTION_NAMES, DB_HOST, DB_KEY,
            QUERY);
  
    public final static String getDBName(Configuration conf) {
        return conf.get(DB_NAME);
    }

    public final static String[] getInputCollectionNames(Configuration conf) {
        String[] collectionNames = conf.get(INPUT_COLLECTION_NAMES).split(",");
        return collectionNames;
    }

    public final static String[] getOutputCollectionNames(Configuration conf) {
        String[] collectionNames = conf.get(OUTPUT_COLLECTION_NAMES).split(",");
        return collectionNames;
    }

    public final static String getDBEndpoint(Configuration conf) {
        return conf.get(DB_HOST);
    }

    public final static String getDBKey(Configuration conf) {
        return conf.get(DB_KEY);
    }

    public final static String getQuery(Configuration conf) {
        return conf.get(QUERY);
    }
    
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

    public final static boolean getUpsert(Configuration conf) {
        String upsert = conf.get(UPSERT);
        return (upsert != null && upsert.equalsIgnoreCase("false")) ? false : true;
    }
    
    public static void copyDocumentDBProperties(Properties from, Map<String, String> to) {
        for (String key : ALL_PROPERTIES) {
            String value = from.getProperty(key);
            if (value != null) {
                to.put(key, value);
            }
        }
    }
}
