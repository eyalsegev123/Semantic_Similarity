import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.io.InputStreamReader;

public class Step4 {

    public static class MapperClass4 extends Mapper<LongWritable, Text, Text, Text> {
        
        private HashMap<String, HashSet<String>> goldenPairs = new HashMap<>(); // maps every word in the word-relatedness.txt to a set of the words it comes with in pairs
        long countF;
        long countL;

        public void setup(Context context) throws IOException, InterruptedException {
            countF = context.getConfiguration().getLong("countF", 0);
            countL = context.getConfiguration().getLong("countL", 0);
            // Configure AWS client using instance profile credentials (recommended when
            // running on AWS infrastructure)
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion("us-east-1") // Specify your bucket region
                    .build();

            String bucketName = "mori_verabi"; // Your S3 bucket name
            String key = "word-relatedness.txt"; // S3 object key for the stopwords file

            try {
                S3Object s3object = s3Client.getObject(bucketName, key);
                try (S3ObjectInputStream inputStream = s3object.getObjectContent();
                        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] fields = line.split("\t");
                        String word1 = fields[0];
                        String word2 = fields[1];

                        if(goldenPairs.get(word1) != null){
                            goldenPairs.get(word1).add(word2);
                        }
                        else{
                            goldenPairs.put(word1 , new HashSet<String>());
                            goldenPairs.get(word1).add(word2);
                        }


                        if(goldenPairs.get(word2) != null) {
                            goldenPairs.get(word2).add(word1);
                        }
                        else {
                            goldenPairs.put(word2 , new HashSet<String>());
                            goldenPairs.get(word2).add(word1);
                        }
                        
                    }
                }
            } catch (Exception e) {
                // Handle exceptions properly in a production scenario
                e.printStackTrace();
            }
        }


        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t"); // Split by tab
            if(fields.length == 0){
                return;
            }

            String headWord = fields[0];
            long count_F_is_f = 0;
            long count_L_is_l = 0;

            HashMap<String, Double> featuresByCount = new HashMap<>();
        
            for(int i=1; i<fields.length; i++) {
                String[] fieldsOfFeature = fields[i].split(":");
                String featureWord = fieldsOfFeature[0];
                Double featureCount = Double.parseDouble(fieldsOfFeature[1]);
                if(featureWord.equals(headWord)) {
                    featuresByCount.putIfAbsent(featureWord, featureCount);
                }
            }

            HashMap<String, Double> prob_by_method_5 = prob_by_method_5(featuresByCount);
            HashMap<String, Double> prob_by_method_6 = prob_by_method_6(featuresByCount);
            HashMap<String, Double> prob_by_method_7 = prob_by_method_7(prob_by_method_6);
            HashMap<String, Double> prob_by_method_8 = prob_by_method_8(featuresByCount);
            

        }

        protected HashMap<String, Double> prob_by_method_5(HashMap<String, Double> featuresByCount) {
            return featuresByCount;
        }

        protected HashMap<String, Double> prob_by_method_6(HashMap<String, Double> featuresByCount) {
            for(Map.Entry<String, Double> entry : featuresByCount.entrySet()) {
                featuresByCount.put(entry.getKey(), (double)( (Math.log((entry.getValue()/countL))) / (Math.log(2)) ));
            }
            return featuresByCount;
        }
        
        protected HashMap<String, Double> prob_by_method_7(HashMap<String, Double> featuresByProbsOfMethod_6) {
            for(Map.Entry<String, Double> entry : featuresByProbsOfMethod_6.entrySet()) {
                featuresByProbsOfMethod_6.put(entry.getKey(), (double) (entry.getValue() / countL));
            }
            return featuresByProbsOfMethod_6;
        }

        protected HashMap<String, Double> prob_by_method_8(HashMap<String, Double> featuresByCount) {
            return featuresByCount;
        }
    }

    public static class ReducerClass4 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            
        }

        protected double[] dist_by_method_9(String[] featuresArrayByCount) {
            return 0.0;
        }

        protected double[] dist_by_method_10() {
            return 0.0;
        }
        
        protected double[] dist_by_method_11() {
            return 0.0;
        }

        protected double[] dist_by_method_13() {
            return 0.0;
        }
        
        protected double[] dist_by_method_15() {
            return 0.0;
        }

        protected double[] dist_by_method_17() {
            return 0.0;
        }
        
    }

    public static class PartitionerClass4 extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return 1;
            // change it
        }
    }

    public static void main(String[] args) throws Exception {
        
        System.out.println("[DEBUG] STEP 4 started!");
        String bucketName = "mori-verabi";
        
        //Step 1: Initialize Configuration
        Configuration conf = new Configuration();

        // Step 2: Retrieve the counter value from S3
        String counterF_FilePath = "s3://" + bucketName + "/output/counters/F.txt";
        String counterL_FilePath = "s3://" + bucketName + "/output/counters/L.txt";
        
        // Step 3: Read the counter value from the file in S3
        FileSystem fs = FileSystem.get(new URI("s3://" + bucketName), conf);
        Path counterPath_F = new Path(counterF_FilePath);
        Path counterPath_L = new Path(counterL_FilePath);

        FSDataInputStream in = fs.open(counterPath_F);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line;
        long countF = 0 , countL = 0;
        
        // Parse the counter value from the file
        while ((line = br.readLine()) != null) {
            if (line.contains("Counter F Value:")) {
                String[] parts = line.split(":");
                countF = Long.parseLong(parts[1].trim());
            }
        }

        in = fs.open(counterPath_L);
        br = new BufferedReader(new InputStreamReader(in));
        
        // Parse the counter value from the file
        while ((line = br.readLine()) != null) {
            if (line.contains("Counter L Value:")) {
                String[] parts = line.split(":");
                countL = Long.parseLong(parts[1].trim());
            }
        }
        br.close();
        in.close();

        Job job = Job.getInstance(conf, "Step4");
        job.setJarByClass(Step4.class);
        job.setMapperClass(MapperClass4.class);
        job.setPartitionerClass(PartitionerClass4.class);
        job.setReducerClass(ReducerClass4.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().setLong("countF", countF);
        job.getConfiguration().setLong("countL", countL);

        // For n_grams S3 files.
        // Note: This is English version and you should change the path to the relevant
        // one
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://" + bucketName + "/output/step3"));
        TextOutputFormat.setOutputPath(job, new Path("s3://" + bucketName + "/output/step4"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}