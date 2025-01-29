import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import java.net.URI;
import org.apache.hadoop.conf.Configuration;

public class Step3 {

    public static class MapperClass3 extends Mapper<LongWritable, Text, Text, Text> {
        private HashSet<String> goldenWords = new HashSet<>();
        private static enum Counters {
            COUNT_F, COUNT_L;
        }

        
        //The format the we get in the input file is: <headWord  TAB nGram (seperated by SPACES)  TAB totalCount>
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t"); // Split by tab

            if(fields.length < 3) {
                System.out.print("wrong format of line -- fields < 3");
                return;
            }

            String headWord = fields[0];

            if(!goldenWords.contains(headWord)) {
                return;
            }
            
            String nGram = fields[1];
            String featureCount = fields[2];
            context.getCounter(Counters.COUNT_L).increment(Integer.parseInt(featureCount));
            String[] nGramArray = nGram.split(" "); //Seperate the nGram by space
            String newValueToWrite = "";
            
            for(int i = 1; i < nGramArray.length; i++) { 
                String[] fieldsOfFeature = nGramArray[i].split("/");
                String featureWord = fieldsOfFeature[0] ;
                String featureRelation = fieldsOfFeature[2];
                String feature = featureWord + "-" + featureRelation;
                if(goldenWords.contains(fieldsOfFeature[0])) {
                    newValueToWrite +=  feature + ":" + featureCount + "/t";
                    context.getCounter(Counters.COUNT_F).increment(Integer.parseInt(featureCount));
                }
            }
            newValueToWrite += headWord + ":" + featureCount; // adding in the end the lexeme itself (cease for example) without "-relation"
            context.write(new Text(headWord) , new Text(newValueToWrite));
        }

        

    }
    
    
    
    public static class ReducerClass3 extends Reducer<Text, Text, Text, Text> {
        private HashMap<String, Integer> currentHeadWordFeatures;
        private String currentHeadWord = null;
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String keyString = key.toString().trim();
            String[] features = values.toString().split("/t"); // Split the values by tab
            
            if(currentHeadWord != null && currentHeadWord.equals(keyString)) { // Same Word
                for(String feature : features){
                    String[] featureFields = feature.split(":");
                    Integer count = Integer.parseInt(featureFields[1]);
                    Integer oldValue = currentHeadWordFeatures.getOrDefault(keyString, 0);
                    currentHeadWordFeatures.put(featureFields[0], oldValue  + count);
                }
            }
            // Initialize or reset HashMap when first word changes
            else if(currentHeadWord == null) { // First word
                currentHeadWord = keyString;
                currentHeadWordFeatures = new HashMap<>();
            }
            else if (!currentHeadWord.equals(keyString)) { // New word 
                String newValueToWrite = "";
                for(Map.Entry<String, Integer> entry : currentHeadWordFeatures.entrySet()) {
                    newValueToWrite += entry.getKey() + ":" + entry.getValue().toString() + "/t";
                }
                context.write(new Text(currentHeadWord), new Text(newValueToWrite));
                currentHeadWord = keyString;
                currentHeadWordFeatures = new HashMap<>();     
            }   
        }
    }

    public static class PartitionerClass3 extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step3");
        job.setJarByClass(Step3.class);
        job.setMapperClass(MapperClass3.class);
        job.setPartitionerClass(PartitionerClass3.class);
        job.setReducerClass(ReducerClass3.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String bucketName = "mori-verabi"; // Your S3 bucket name
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // SequenceFileInputFormat.addInputPath(job,
        //         new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data"));
        // SequenceFileInputFormat.addInputPath(job,
        //         new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data"));
        // SequenceFileInputFormat.addInputPath(job,
        //         new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
        TextOutputFormat.setOutputPath(job, new Path("s3://" + bucketName + "/output/step3"));
        
        boolean success = job.waitForCompletion(true);
        
        if (success) {
            // Retrieve and save the counter value after job completion
            long countF = job.getCounters()
                            .findCounter(MapperClass3.Counters.COUNT_F)
                            .getValue();

            long countL = job.getCounters()
                            .findCounter(MapperClass3.Counters.COUNT_L)
                            .getValue();

            // Create an S3 path to save the counter value
            String counterF_FilePath = "s3://" + bucketName + "/output/counters/F.txt";
            String counterL_FilePath = "s3://" + bucketName + "/output/counters/L.txt";

            // Write the counter value to the file in S3
            FileSystem fs = FileSystem.get(new URI("s3://" + bucketName), conf);
            Path counterPath = new Path(counterF_FilePath);

            FSDataOutputStream outF = fs.create(counterPath);
            outF.writeBytes("Counter F Value:" + countF + "\n");
            outF.close();

            counterPath = new Path(counterL_FilePath);
            FSDataOutputStream outL = fs.create(counterPath);
            outL.writeBytes("Counter L Value:" + countL + "\n");
            outL.close();
        }
        System.exit(success ? 0 : 1);
    }
}