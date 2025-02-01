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

        
        //The format the we get in the input file is: <headWord  TAB features (seperated by SPACES)  TAB totalCount>
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
            
            String features = fields[1];
            String generalCount = fields[2];
            context.getCounter(Counters.COUNT_L).increment(Integer.parseInt(generalCount));
            String[] featureArray = features.split(" "); //Seperate the features by space
            String newValueToWrite = "";
            
            for(int i = 1; i < featureArray.length; i++) { 
                String[] fieldsOfFeature = featureArray[i].split("/");
                String featureWord = fieldsOfFeature[0] ;
                String featureRelation = fieldsOfFeature[1];
                String count_F_is_f = fieldsOfFeature[2];
                String finalFeatureWithCount = featureWord + "-" + featureRelation + "/" + count_F_is_f;  // featureWord-relation/Count_F_is_f
                newValueToWrite += finalFeatureWithCount + " ";
                context.getCounter(Counters.COUNT_F).increment(Integer.parseInt(generalCount));
                
            }
            //We send:
            //Key: headWord
            //Value: <feature1-POS/count_f_is_F....  <TAB>  generalCount>
            context.write(new Text(headWord) , new Text(newValueToWrite + "\t" + generalCount)); 
        }
    }
    

    // The reducer will get: Iterable of sentences of headWord
    // Key: headWord
    // Value: <feature1-POS/count_f_is_F....  <TAB>  generalCount> , value2 .....

    //We will send to Mapper4: 
    //Key: headWord
    //Value: <feature1-POS/count_f_is_F/count_f_With_l ..... > TAB count_L_is_l
    public static class ReducerClass3 extends Reducer<Text, Text, Text, Text> {
    
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String headWord = key.toString().trim();
            HashMap<String, Integer> currentHeadWordFeaturesToCount = new HashMap<>();
            long count_L_is_l = 0; // Total count of the headWord


            for(Text value : values) {
                count_L_is_l += Long.parseLong(value.toString().split("\t")[1]);
            }

            
            for(Text value : values) {
                
                String[] valueFields = value.toString().split("\t");
                String[] features = valueFields[0].split(" ");
                int generalCount = Integer.parseInt(valueFields[1]);

                for(String feature : features){
                    String[] featureFields = feature.split("/");
                    String featureWord = featureFields[0];
                    String count_F_is_f = featureFields[1];
                    int current_count_f_with_l = currentHeadWordFeaturesToCount.getOrDefault(featureWord + "\t" + count_F_is_f, 0);
                    currentHeadWordFeaturesToCount.put(featureWord + "\t" + count_F_is_f , current_count_f_with_l + generalCount); 
                }
            }

            String newValueToWrite = "";
            for(Map.Entry<String, Integer> entry : currentHeadWordFeaturesToCount.entrySet()) {
                String featureWord = entry.getKey().split("\t")[0];
                String count_F_is_f = entry.getKey().split("\t")[1];
                String count_f_with_l = entry.getValue().toString();

                newValueToWrite += featureWord + "/" + count_f_with_l + "/" + count_F_is_f + " ";
                // we add every feature to the value in this type:
                //word-relation/count(f,l)/count(F=f)
            }
            context.write(new Text(headWord) , new Text(newValueToWrite + "\t" + count_L_is_l));

        }
    }

    public static class PartitionerClass3 extends Partitioner<Text, Text> {
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
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String bucketName = "mori-verabi"; // Your S3 bucket name
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path("s3://" + bucketName + "/output/step2"));
        TextOutputFormat.setOutputPath(job, new Path("s3://" + bucketName + "/output/step3"));
        
        boolean success = job.waitForCompletion(true);
        
        if (success) { // We need it in next phases?????!??!?!??!??!?!?
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