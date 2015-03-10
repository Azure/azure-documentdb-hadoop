//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------
package com.microsoft.azure.documentdb.hadoop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DocumentDBWritableComparator extends WritableComparator {

    public DocumentDBWritableComparator() {
        super(DocumentDBWritable.class, true);
    }

    protected DocumentDBWritableComparator(
            final Class<? extends WritableComparable> keyClass) {
        super(keyClass, true);
    }

    protected DocumentDBWritableComparator(
            final Class<? extends WritableComparable> keyClass,
            final boolean createInstances) {
        super(keyClass, createInstances);
    }

    /**
     * @inheritDoc
     */
    @Override
    public int compare(final WritableComparable a, final WritableComparable b) {
        if (a instanceof DocumentDBWritable && b instanceof DocumentDBWritable) {
            return new DocumentDBWritableComparator().compare(
                    ((DocumentDBWritable) a).getDoc(),
                    ((DocumentDBWritable) b).getDoc());
        } else {
            return super.compare(a, b);
        }
    }
}
