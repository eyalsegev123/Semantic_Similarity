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
import java.util.HashSet;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class Step1 {

    public static class MapperClass1 extends Mapper<LongWritable, Text, Text, Text> {
        private HashSet<String> goldenWords = new HashSet<>();
        
        protected void setup(Context context) throws IOException, InterruptedException {
            // Configure AWS client using instance profile credentials (recommended when
            // running on AWS infrastructure)
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion("us-east-1") // Specify your bucket region
                    .build();

            String bucketName = "mori-verabi"; // Your S3 bucket name
            String key = "word-relatedness.txt"; // S3 object key for the word-relatedness file

            try {
                S3Object s3object = s3Client.getObject(bucketName, key);
                try (S3ObjectInputStream inputStream = s3object.getObjectContent();
                        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] fields = line.split("\t");
                        String word1 = fields[0];
                        String word2 = fields[1];
                        goldenWords.add(word1);
                        goldenWords.add(word2); // Needs to be Stemmed???????
                    }
                }
            } catch (Exception e) {
                // Handle exceptions properly in a production scenario
                System.err.println("Exception while reading golden words from S3: " + e.getMessage());
                e.printStackTrace();
            }
        }


        //The format the we get in the input file is: <headWord  TAB nGram (seperated by SPACES)  TAB totalCount>
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t"); // Split by tab
            String headWord = fields[0];

            if(!goldenWords.contains(headWord)) {
                return;
            }

            String nGram = fields[1];
            String[] nGramArray = nGram.split(" ");

            String[] nGramArrayWithOutHeadWord = new String[nGramArray.length - 1];
            int index_to_insert = 0;
            for(int i = 0; i < nGramArray.length; i++) {
                String featureWord = nGramArray[i].split("/")[0];
                if(featureWord.equals(headWord)){
                    continue;
                }
                nGramArrayWithOutHeadWord[index_to_insert] = nGramArray[i];
                index_to_insert++;
            }

            String featureCount = fields[2];
            String valueToWrite = headWord + "\t" + nGramArrayWithOutHeadWord + "\t" + featureCount;
            for(int i = 0; i < nGramArrayWithOutHeadWord.length; i++) { 
                String[] fieldsOfFeature = nGramArrayWithOutHeadWord[i].split("/");
                String featureWord = fieldsOfFeature[0];
                String featureRelation = fieldsOfFeature[2];
                String feature = featureWord + "-" + featureRelation;
                context.write(new Text(feature), new Text(valueToWrite));
            }
        }


        public String stem(String str) {
            //*
            //*
            // check how to stemm
            //
            //
            return str; 
        }
    }
    
    
    
    public static class ReducerClass1 extends Reducer<Text, Text, Text, Text> {
    
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String featureKey = key.toString().trim();
            long count_F_is_f = 0;
            
            for(Text value : values) {
                String stringValue = value.toString();
                count_F_is_f += Long.parseLong(stringValue.split("\t")[2]);
            }

            for(Text value : values) {
                String stringValue = value.toString();
                String[] fields = stringValue.split("\t");
                String originalHeadWordOfFeature = fields[0];
                String valueToWrite = "";
                String[] ngramArray = fields[1].split(" ");
                String featureCount = fields[2];
                for(String feature : ngramArray) {
                    String[] fieldsOfFeature = feature.split("/"); //Splitting the feature by "/"
                    if(fieldsOfFeature[0].equals(featureKey))
                        valueToWrite += fieldsOfFeature[0] + "/" + fieldsOfFeature[2] + "/" + count_F_is_f + " ";
                    else
                        valueToWrite += fieldsOfFeature[0] + "/" + fieldsOfFeature[2] + " ";        
                }
                context.write(new Text(originalHeadWordOfFeature), new Text(valueToWrite + "\t" + featureCount));
            }
        }
    }

    public static class PartitionerClass1 extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step1");
        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass1.class);
        job.setPartitionerClass(PartitionerClass1.class);
        job.setReducerClass(ReducerClass1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
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
        TextOutputFormat.setOutputPath(job, new Path("s3://" + bucketName + "/output/step1"));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}