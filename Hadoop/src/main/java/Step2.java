import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashSet;


//Reunite the updated features with their original keys 
public class Step2 {

    public static class MapperClass2 extends Mapper<LongWritable, Text, Text, Text> {
        
        
        //The format the we get in the input file is: <headWord  TAB Features (seperated by SPACES)  TAB totalCount>
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t"); // Split by tab
            String headWord = fields[0];
            String[] featureArray = fields[1].split(" ");
            String count = fields[2];
            String sentenceOfHeadWord = "";
            //Extracting sentence of head word
            for(int i=0; i<featureArray.length; i++) {
                String wordOfFeature = featureArray[i].split("/")[0];
                sentenceOfHeadWord += wordOfFeature + " ";
            } 

            //we will send the key: head-word TAB sentenceOfHeadWord --> this way, in the reducer, we will connect between the count_F_is_f of each feature of the sentence.
            String featuresAndCount = fields[1] + "\t" + count;
            context.write(new Text(headWord + "\t" + sentenceOfHeadWord) , new Text(featuresAndCount)); 
        }

    }
    
    
    
    public static class ReducerClass2 extends Reducer<Text, Text, Text, Text> {
       
          
        //Here we will get: 
        //Key: <headword TAB sentenceOfHeadWord> 
        //Value: <feature1/POS .... featurei/POS/count_F_is_f ... featureN/POS   TAB  count>
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            String[] keyFields = key.toString().split("\t");
            String headWord = keyFields[0];
            HashSet<String> totalCountsOfFeatures = new HashSet<>();
            String generalCount = "";
            

            //Iterating over the values to find features with count_F_is_f, adding them in to the features HashSet
            for(Text value : values) {
                String[] featuresArray = value.toString().split("\t")[0].split(" ");
                generalCount = value.toString().split("\t")[1];
                for(String feature : featuresArray){
                    String[] featureFields = feature.split("/");
                    if(featureFields.length < 3){
                        continue;
                    }
                    totalCountsOfFeatures.add(feature);
                }
            }
            //Making a string of all the features with count_F_is_f (seperated by space)
            String featuresWithCount_F_is_f = "";
            for(String feature : totalCountsOfFeatures){
                featuresWithCount_F_is_f += feature + " ";
            }
            
            //We will write: 
            //key: headword
            //value: feature-1/relation/count_F_is_f  <space>    .... <space>  feature-k/relation/count_F_is_f       TAB        general_count_of_sentence  
            context.write(new Text(headWord) , new Text(featuresWithCount_F_is_f + "\t" + generalCount));
        }
    }

    public static class PartitionerClass2 extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
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
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String bucketName = "teacherandrabi"; // S3 bucket name
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path("s3://" + bucketName + "/output/step1"));
        TextOutputFormat.setOutputPath(job, new Path("s3://" + bucketName + "/output/step2"));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}