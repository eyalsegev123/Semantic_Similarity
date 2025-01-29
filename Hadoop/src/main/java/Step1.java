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
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class Step1 {

    public static class MapperClass1 extends Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        private HashSet<String> goldenWords = new HashSet<>();

        protected void setup(Context context) throws IOException, InterruptedException {
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

            if(fields.length < 3) {
                System.out.print("wrong format of line -- fields < 3");
                return;
            }
            
            String headWord = fields[0];
            String nGram = fields[1];
            String featureCount = fields[2];
            String[] nGramArray = nGram.split(" "); //Seperate the nGram by space
            String newValueToWrite = "";
            
            for(int i = 1; i < nGramArray.length; i++) { 
                String[] fieldsOfFeature = nGramArray[i].split("/");
                String featureWord = stemm(fieldsOfFeature[0]) ;
                String featureRelation = fieldsOfFeature[2];
                String feature = featureWord + "-" + featureRelation;
                if(goldenWords.contains(fieldsOfFeature[0])) {
                    newValueToWrite +=  feature + ":" + featureCount + "/t";
                }
            }
            context.write(new Text(headWord) , new Text(newValueToWrite));
        }

        public String stemm(String str) {
            //*
            //*
            // check how to stemm
            //
            //
            return str; 
        }

    }

    public static class ReducerClass1 extends Reducer<Text, IntWritable, Text, Text> {
        private HashMap<String, Integer> currentWordGrams;
        private String currentFirstWord = null;
        
        private static enum Counters {
            C0 // Define a custom counter
        }


        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            String keyString = key.toString().trim();
            String[] words = keyString.split(" "); // Split the key by space
            String firstWord = words[0];

            // Write to log file
            System.out.println("Working on key: " + keyString);

            // Initialize or reset HashMap when first word changes
            if (currentFirstWord == null || !currentFirstWord.equals(firstWord)) {
                if (currentWordGrams != null) {
                    System.out.println("Key has been changed from: " + currentFirstWord + " to: " + firstWord
                            + ". Passing HashMap.");

                    for (Map.Entry<String, Integer> entry : currentWordGrams.entrySet()) {
                        String ngram = entry.getKey();
                        System.out.println("Current n-gram in HashMap: " + ngram);
                        String[] ngramWords = ngram.split(" ");
                        int n = ngramWords.length;
                        String w1 = ngramWords[0];

                        String sum = entry.getValue().toString();
                        if (n == 1) {
                            context.write(new Text(ngram), new Text(ngram + ":" + sum));
                        } else if (n == 2) {
                            Integer w1SumInteger = currentWordGrams.get(w1);
                            String w1Sum = w1SumInteger != null ? w1SumInteger.toString() : "null";
                            context.write(new Text(ngram), new Text(w1 + ":" + w1Sum + "\t" + ngram + ":" + sum));
                        } else if (n == 3) {
                            String w2 = ngramWords[1];
                            Integer w1SumInteger = currentWordGrams.get(w1);
                            String w1Sum = w1SumInteger != null ? w1SumInteger.toString() : "null";
                            Integer w1w2SumInteger = currentWordGrams.get(w1 + " " + w2);
                            String w1w2Sum = w1w2SumInteger != null ? w1w2SumInteger.toString() : "null";
                            context.write(new Text(ngram), new Text(w1 + ":" + w1Sum + "\t" + w1 + " " + w2 + ":"
                                    + w1w2Sum + "\t" + ngram + ":" + sum));
                        }
                    }
                }
                currentFirstWord = firstWord;
                currentWordGrams = new HashMap<>();
            }

            // Sum up the counts for this n-gram
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            
            context.getCounter(Counters.C0).increment(sum);

            // Store in HashMap
            currentWordGrams.put(keyString, sum);
            System.out.println("Stored in HashMap: <" + keyString + " , " + sum + ">");
        }

        
    }

    public static class PartitionerClass1 extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            String ngram = key.toString();
            String firstWord = ngram.split(" ")[0];
            return Math.abs(firstWord.hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step1 - Word Counting");
        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass1.class);
        job.setPartitionerClass(PartitionerClass1.class);
        // job.setSortComparatorClass(NgramComparator.class);
        // job.setCombinerClass(ReducerClass1.class);
        job.setReducerClass(ReducerClass1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // For n_grams S3 files.
        // Note: This is English version and you should change the path to the relevant
        // one
        String bucketName = "hashem-itbarach"; // Your S3 bucket name
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job,
                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data"));
        SequenceFileInputFormat.addInputPath(job,
                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data"));
        SequenceFileInputFormat.addInputPath(job,
                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
        TextOutputFormat.setOutputPath(job, new Path("s3://" + bucketName + "/output/step1"));
        
        boolean success = job.waitForCompletion(true);
        
        if (success) {
            // Retrieve and save the counter value after job completion
            long c0Value = job.getCounters()
                            .findCounter(ReducerClass1.Counters.C0)
                            .getValue();
            System.out.println("Counter C0 Value: " + c0Value);

            // Create an S3 path to save the counter value
            String counterFilePath = "s3://" + bucketName + "/output/step1/counter_c0.txt";

            // Write the counter value to the file in S3
            FileSystem fs = FileSystem.get(new URI("s3://" + bucketName), conf);
            Path counterPath = new Path(counterFilePath);
            FSDataOutputStream out = fs.create(counterPath);
            out.writeBytes("Counter C0 Value: " + c0Value + "\n");
            out.close();
        }
        System.exit(success ? 0 : 1);
    }

}