package com.microsoft.azure.documentdb.mapred.hadoop;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;

import com.microsoft.azure.documentdb.hadoop.DocumentDBInputSplit;

/**
 * A split in the old mapred.* API that represents a split from Documentdb.
 * Implemented here as a FileSplit because Hive misbehaves if you give it other
 * types of splits.
 */
@SuppressWarnings("deprecation")
public class WrapperSplit extends FileSplit implements Writable {
    private DocumentDBInputSplit wrappedSplit;

    /**
     * A parameter-less constructor for deserialization.
     */
    public WrapperSplit() {
        super((Path) null, 0, 0, (String[]) null);
    }

    /**
     * Create a split to wrap a given documentdb split.
     * 
     * @param wrappedSplit
     *            The split to wrap.
     * @param file
     *            The file path for the partition in Hive (needs to match).
     * @param conf
     *            The configuration.
     */
    public WrapperSplit(DocumentDBInputSplit wrappedSplit, Path file, JobConf conf) {
        super(file, 0, 0, conf);
        this.wrappedSplit = wrappedSplit;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        String className = in.readUTF();
        try {
            wrappedSplit = (DocumentDBInputSplit) ReflectionUtils.newInstance(Class.forName(className),
                    new Configuration());
        } catch (Exception e) {
            throw new IOException(e);
        }
        wrappedSplit.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeUTF(wrappedSplit.getClass().getName());
        wrappedSplit.write(out);
    }

    public DocumentDBInputSplit getWrappedSplit() {
        return wrappedSplit;
    }
}