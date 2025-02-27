import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.LinkedList;

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

//caclculating count F is f for each feature in the corpus
public class Step1 {

    public static class MapperClass1 extends Mapper<LongWritable, Text, Text, Text> {
        
        private HashSet<String> goldenWords = new HashSet<>(); //Stemmed golden words

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
                        String[] fields = line.split("\\s+"); // Split by any whitespace (TAB or SPACE)
                    
                        // Ensure the line has at least two words
                        if (fields.length < 2) continue;

                        String word1 = fields[0].trim();
                        String word2 = fields[1].trim();

                        if (!word1.isEmpty()) goldenWords.add(stem(word1));
                        if (!word2.isEmpty()) goldenWords.add(stem(word2));
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
            
            if(fields.length < 3) return;
            
            String headWord = fields[0].trim();
            String headWordStemmed = stem(headWord);
            String nGram = fields[1];

            if(!goldenWords.contains(headWordStemmed)) return;

            String[] nGramArray = nGram.split("\\s+"); //split by spaces the features
            if(nGramArray.length == 0) return;
            

            LinkedList<String> nGramArrayWithOutHeadWord = new LinkedList<>();
 
            //Remove the head-word's feature from the nGramArray
            for(int i = 0; i < nGramArray.length; i++) {
                String featureWord = nGramArray[i].split("/")[0];
                if(featureWord.equals(headWord))
                    continue;
                nGramArrayWithOutHeadWord.add(nGramArray[i]);
            }
            
            if(nGramArrayWithOutHeadWord.size() == 0) return;
            

            String nGramArrayWithOutHeadWordString = "";
            for (String feature : nGramArrayWithOutHeadWord) {
                nGramArrayWithOutHeadWordString += feature + " ";
            }
            nGramArrayWithOutHeadWordString = nGramArrayWithOutHeadWordString.substring(0, nGramArrayWithOutHeadWordString.length() - 1); //removing the last space
            
            String featureCount = fields[2];
            String valueToWrite = headWordStemmed + "\t" + nGramArrayWithOutHeadWordString + "\t" + featureCount;
            
            for(int i = 0; i < nGramArrayWithOutHeadWord.size(); i++) {
                String[] fieldsOfFeature = nGramArrayWithOutHeadWord.get(i).split("/");
                String featureWord = fieldsOfFeature[0];
                String featureRelation = fieldsOfFeature[2];
                String feature = featureWord + "-" + featureRelation;
                context.write(new Text(feature), new Text(valueToWrite));
            }
        }

        public String stem(String word) {
            Stemmer stemmer = new Stemmer();
            stemmer.add(word.toCharArray(), word.length()); // Add the full word
            stemmer.stem(); // Perform stemming
            return stemmer.toString();
        }
    }
    
    public static class ReducerClass1 extends Reducer<Text, Text, Text, Text> {
    
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
       
            String featureKey = key.toString().trim();

            String featureWord = featureKey.split("-")[0].trim();
            long count_F_is_f = 0;
            
            HashSet<String> valuesSet = new HashSet<>();
            //Summing the count_F_is_f for the feature
            for(Text value : values) {
                String stringValue = value.toString();
                Long generalCount = Long.parseLong(stringValue.split("\t")[2]);
                count_F_is_f += generalCount;
                valuesSet.add(value.toString());
            }

            //For each feature, we will turn the key-value back to how it used to be in the input, but with adding the count_F_is_f to the specific feature
            for(String value : valuesSet) {
                String[] fields = value.split("\t");
                String originalHeadWordOfFeature = fields[0];
                String[] ngramArray = fields[1].split(" ");
                String featureCount = fields[2];
                String valueToWrite = "";
                
                for(String feature : ngramArray) {
                    String[] fieldsOfFeature = feature.split("/"); //Splitting the feature by "/"
                    if(fieldsOfFeature[0].trim().equals(featureWord))
                        valueToWrite += fieldsOfFeature[0] + "/" + fieldsOfFeature[2] + "/" + count_F_is_f + " ";
                    else
                        valueToWrite += fieldsOfFeature[0] + "/" + fieldsOfFeature[2] + " ";        
                }
                valueToWrite = valueToWrite.substring(0, valueToWrite.length() - 1);
                context.write(new Text(originalHeadWordOfFeature), new Text(valueToWrite + "\t" + featureCount));
            }       
        }
    }

    public static class PartitionerClass1 extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.toString().hashCode()) % numPartitions;
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
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://teacherandrabi/biarcs10")); // Path to the input that come from Google N-Grams
        TextOutputFormat.setOutputPath(job, new Path("s3://" + bucketName + "/output/step1"));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}   

