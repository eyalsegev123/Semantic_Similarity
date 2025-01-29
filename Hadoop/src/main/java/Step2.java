import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class Step2 {

    public static class MapperClass2 extends Mapper<LongWritable, Text, Text, Text> {
        
        
        //The format the we get in the input file is: <headWord  TAB nGram (seperated by SPACES)  TAB totalCount>
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t"); // Split by tab
            String headWord = fields[0];
            String valueToWrite = "";
            for(int i=1; i<fields.length; i++) {
                valueToWrite += fields[i];
            }   
            context.write(new Text(headWord) , new Text(valueToWrite)); 
        }

    }
    
    
    
    public static class ReducerClass2 extends Reducer<Text, Text, Text, Text> {
       
                

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            String headWord = key.toString();
            HashSet<String> totalCountsOfFeatures = new HashSet<>();
            String featureCount = "";
            for(Text value : values) {
                for(String feature : value.toString().split(" ")){
                    String[] featureFields = feature.split("/");
                    if(featureFields.length < 3){
                        continue;
                    }
                    totalCountsOfFeatures.add(feature);
                }
            }
            String newValueToWrite = "";
            for(String feature : totalCountsOfFeatures){
                newValueToWrite += feature + " ";
            }
            context.write(new Text(headWord) , new Text(newValueToWrite + "/t" + featureCount));
        }
    }

    public static class PartitionerClass2 extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step2");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass2.class);
        job.setPartitionerClass(PartitionerClass2.class);
        job.setReducerClass(ReducerClass2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String bucketName = "mori-verabi"; // Your S3 bucket name
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path("s3://" + bucketName + "/output/step1"));
        TextOutputFormat.setOutputPath(job, new Path("s3://" + bucketName + "/output/step2"));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}