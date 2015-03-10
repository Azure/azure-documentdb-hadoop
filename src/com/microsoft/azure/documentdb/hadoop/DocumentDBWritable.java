//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
package com.microsoft.azure.documentdb.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.microsoft.azure.documentdb.Document;

public class DocumentDBWritable implements WritableComparable<Object> {

    static {
        WritableComparator.define(DocumentDBWritable.class,
                new DocumentDBWritableComparator());
    }

    private Document doc;

    public DocumentDBWritable() {
        this.doc = new Document();
    }

    public DocumentDBWritable(final Document doc) {
        this.setDoc(doc);
    }

    public void setDoc(final Document doc) {
        this.doc = doc;
    }

    public Document getDoc() {
        return this.doc;
    }

    /**
     * {@inheritDoc}
     * 
     * @see Writable#write(DataOutput)
     */
    public void write(final DataOutput out) throws IOException {
        String str = this.doc.toString();
        out.writeUTF(str);
    }

    /**
     * {@inheritDoc}
     * 
     * @see Writable#readFields(DataInput)
     */
    public void readFields(final DataInput in) throws IOException {
        this.doc = new Document(in.readLine());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return this.doc.toString();
    }

    public int compareTo(final Object o) {
        return new DocumentDBWritableComparator().compare(this, o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return this.doc != null ? this.doc.hashCode() : 0;
    }
}
