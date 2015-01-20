//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

package com.microsoft.azure.documentdb.pig;

import java.util.HashMap;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.parser.ParserException;
import org.apache.pig.parser.QueryParserDriver;

class SchemaHelper {
    /**
     * @param schemaString a String representation of the Schema <b>without</b>
     *                     any enclosing curly-braces.<b>Not</b> for use with
     *                     <code>Schema#toString</code>
     * @return Schema instance
     * @throws ParserException
     */
    public static Schema getSchemaFromString(String schemaString) throws ParserException {
        LogicalSchema schema = parseSchema(schemaString);
        Schema result = org.apache.pig.newplan.logical.Util.translateSchema(schema);
        Schema.setSchemaDefaultType(result, DataType.BYTEARRAY);
        return result;
    }
    
    public static LogicalSchema parseSchema(String schemaString) throws ParserException {
        QueryParserDriver queryParser = new QueryParserDriver( new PigContext(), 
                "util", new HashMap<String, String>() ) ;
        LogicalSchema schema = queryParser.parseSchema(schemaString);
        return schema;
    }
}
