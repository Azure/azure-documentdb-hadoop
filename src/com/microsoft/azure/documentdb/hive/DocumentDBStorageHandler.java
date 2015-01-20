package com.microsoft.azure.documentdb.hive;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.JobConf;

import com.microsoft.azure.documentdb.hadoop.ConfigurationUtil;
import com.microsoft.azure.documentdb.mapred.hadoop.DocumentDBInputFormat;
import com.microsoft.azure.documentdb.mapred.hadoop.DocumentDBOutputFormat;

public class DocumentDBStorageHandler extends Configured implements HiveStorageHandler {
    public DocumentDBStorageHandler() {
        super();
    }

    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        Properties properties = tableDesc.getProperties();
        ConfigurationUtil.copyDocumentDBProperties(properties, jobProperties);
    }

    public Class<? extends InputFormat> getInputFormatClass() {
        return DocumentDBInputFormat.class;
    }

    public HiveMetaHook getMetaHook() {
        return null;
    }

    public Class<? extends OutputFormat> getOutputFormatClass() {
        return DocumentDBOutputFormat.class;
    }

    public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        Properties tableProperties = tableDesc.getProperties();

        for (Entry<Object, Object> entry : tableProperties.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            /* work around for hive 0.13 bug for custom serde that throws an exception
             *  when columns.comments is set to empty string. 
             */
            if(value != null && !value.isEmpty() && !key.equals("columns.comments")) {
                jobProperties.put(key, (String) entry.getValue());
            }
        }
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        Properties tableProperties = tableDesc.getProperties();
        for (Entry<Object, Object> entry : tableProperties.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if(value != null && !value.isEmpty() && !key.equals("columns.comments")) {
                jobProperties.put((String) entry.getKey(), (String) entry.getValue());
            }
        }
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
        return null;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return DocumentDBSerDe.class;
    }
}
