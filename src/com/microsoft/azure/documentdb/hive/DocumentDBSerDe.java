//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

package com.microsoft.azure.documentdb.hive;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.openx.data.jsonserde.JsonSerDe;
import org.openx.data.jsonserde.json.JSONObject;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.hadoop.DocumentDBWritable;

public class DocumentDBSerDe implements SerDe {

    private DocumentDBWritable cachedWritable;
    private JsonSerDe jsonSerde;

    public DocumentDBSerDe() {
        this.cachedWritable = new DocumentDBWritable();
        this.jsonSerde = new JsonSerDe();
    }

    public Object deserialize(Writable writable) throws SerDeException {
        Text txtWritable = new Text(writable.toString());
        return (JSONObject) this.jsonSerde.deserialize(txtWritable);
    }

    public ObjectInspector getObjectInspector() throws SerDeException {
        return this.jsonSerde.getObjectInspector();
    }

    public SerDeStats getSerDeStats() {
        return this.jsonSerde.getSerDeStats();
    }

    public Class<? extends Writable> getSerializedClass() {
        return DocumentDBWritable.class;
    }
    
    public void initialize(Configuration conf, Properties properties) throws SerDeException {
        this.jsonSerde.initialize(conf, properties);
    }

    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
        Text txtWritable = (Text) this.jsonSerde.serialize(obj, objInspector);
        Document doc = new Document(txtWritable.toString());
        this.cachedWritable.setDoc(doc);
        return this.cachedWritable;
    }
}
