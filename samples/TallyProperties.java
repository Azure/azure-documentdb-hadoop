import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.hadoop.ConfigurationUtil;
import com.microsoft.azure.documentdb.hadoop.DocumentDBInputFormat;
import com.microsoft.azure.documentdb.hadoop.DocumentDBOutputFormat;
import com.microsoft.azure.documentdb.hadoop.DocumentDBWritable;

// Tally the number of property occurrences for all Documents in a collection
public class TallyProperties {
    public static class Map extends Mapper<LongWritable, DocumentDBWritable, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, DocumentDBWritable value,
                Context context)
                throws IOException, InterruptedException {
        	
        			// Retrieve all property names from Document
            		Set<String> properties = value.getDoc().getHashMap().keySet();
            		
            		for(String property : properties) {
            			context.write(new Text(property), one);
            		}
        		}
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, DocumentDBWritable> {
        
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = values.iterator();
            
            // Count the number of occurrences for a given property
            while (itr.hasNext()) {
                sum += itr.next().get();
            }

            // Write the property and frequency back into DocumentDB as a document
            Document d = new Document();
            d.set("id", key.toString());
            d.set("frequency", sum);
            context.write(key, new DocumentDBWritable(d));
        }
    }
    

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        final String host = "DocumentDB Endpoint";
        final String key = "DocumentDB Primary Key";
        final String dbName = "DocumentDB Database Name";
        final String inputCollName = "DocumentDB Input Collection Name";
        final String outputCollName = "DocumentDB Output Collection Name";
        conf.set(ConfigurationUtil.DB_HOST, host);
        conf.set(ConfigurationUtil.DB_KEY, key);
        conf.set(ConfigurationUtil.DB_NAME, dbName);
        conf.set(ConfigurationUtil.INPUT_COLLECTION_NAMES, inputCollName);
        conf.set(ConfigurationUtil.OUTPUT_COLLECTION_NAMES, outputCollName);
        
        Job job = Job.getInstance(conf, "TallyProperties");
        job.setJobName("TallyProperties");

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(DocumentDBInputFormat.class);
        job.setOutputFormatClass(DocumentDBOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DocumentDBWritable.class);
        
        job.setJarByClass(TallyProperties.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}