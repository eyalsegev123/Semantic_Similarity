import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//Calculating count_L, count_F, count(l,f)
public class Step3 {

    public static class MapperClass3 extends Mapper<LongWritable, Text, Text, Text> {
        private static enum Counters {
            COUNT_F, COUNT_L;
        }

        
        //The format we get in the input file is: <headWord  TAB features (seperated by SPACES)  TAB totalCount>
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t"); // Split by tab

            if(fields.length < 3) {
                System.out.print("wrong format of line -- fields < 3");
                return;
            }

            String headWord = fields[0];
            String features = fields[1];
            String generalCount = fields[2];

            //Calculating count_L
            context.getCounter(Counters.COUNT_L).increment(Integer.parseInt(generalCount));
            
            String[] featureArray = features.split(" "); //Seperate the features by spaces
            if(featureArray.length == 0) return;
            
            String newValueToWrite = "";
            
            for(int i = 0; i < featureArray.length; i++) {
                String[] fieldsOfFeature = featureArray[i].split("/");
                String featureWord = fieldsOfFeature[0] ;
                String featureRelation = fieldsOfFeature[1];
                String count_F_is_f = fieldsOfFeature[2];
                
                //turn the feature structure to:  featureWord-relation/Count_F_is_f
                String finalFeatureWithCount = featureWord + "-" + featureRelation + "/" + count_F_is_f;

                newValueToWrite += finalFeatureWithCount + " ";

                context.getCounter(Counters.COUNT_F).increment(Integer.parseInt(generalCount));
            }
            //We send:
            //Key: headWord
            //Value: <feature1-POS/count_f_is_F....  <TAB>  generalCount>
            newValueToWrite = newValueToWrite.substring(0, newValueToWrite.length() - 1);
            context.write(new Text(headWord) , new Text(newValueToWrite + "\t" + generalCount)); 
        }
    
    }
    
    // The reducer will get: Iterable of sentences of headWord
    // Key: headWord
    // An Iterable of Values: <feature1-relation/count_f_is_F....  <TAB>  generalCount> , value2 .....

    //We will send to Mapper4: 
    //Key: headWord
    //Value: <feature1-POS/count_f_With_l/count_f_is_F ..... > TAB count_L_is_l
    public static class ReducerClass3 extends Reducer<Text, Text, Text, Text> {
    
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
           
            HashSet<String> valuesSet = new HashSet<>();

            //loop for creating the set and prevent the duplications
            for(Text value : values) {
                String[] valueFields = value.toString().split("\t");
                String features = valueFields[0];
                String generalCount = valueFields[1];
                valuesSet.add(features + "\t" + generalCount);
            }

            String headWord = key.toString().trim();
            HashMap<String, Long> featureToCount_f_with_l = new HashMap<>();
            HashMap<String, Long> featureToCount_F_is_f = new HashMap<>();
            long count_L_is_l = 0; // Total count of the headWord
            
            //Iterating over the values to compute the count_L_is_l
            for(String value : valuesSet) {
                Long generalCount = Long.parseLong(value.split("\t")[1]);
                count_L_is_l += generalCount;
            }

            //Summing the appearences of some f with l from every sentence of l
            for(String value : valuesSet) {
                String[] valueFields = value.split("\t");
                String[] features = valueFields[0].split(" ");
                Long generalCount = Long.parseLong(valueFields[1]);
                //context.write(new Text("[DEBUG]- headword: " + headWord) , new Text("value: " + value));
                for(String feature : features){
                    
                    String[] featureFields = feature.split("/");
                    String featureWordWithRelation = featureFields[0];
                    
                    //Inserting every feature with its count F is f to hashMap
                    long count_F_is_f = Long.parseLong(featureFields[1]);
                    featureToCount_F_is_f.put(featureWordWithRelation , count_F_is_f);
                    
                    //Inserting every feature into the featureToCount_f_with_l
                    long current_count_f_with_l = featureToCount_f_with_l.getOrDefault(featureWordWithRelation, 0L);
                    featureToCount_f_with_l.put(featureWordWithRelation, current_count_f_with_l + generalCount); 
                    //context.write(new Text("[DEBUG]- f = " + featureWordWithRelation + " , l = " + headWord) ,
                    //new Text("current (F is f) = " + featureToCount_F_is_f.get(featureWordWithRelation) + "current (f with l) = " + featureToCount_f_with_l.get(featureWordWithRelation)));
                }
            }

            String newValueToWrite = "";
            
            if(featureToCount_f_with_l.size() == 0 || featureToCount_F_is_f.size() == 0) return;

            for(Map.Entry<String, Long> entry : featureToCount_f_with_l.entrySet()) {
                String featureWordWithRelation = entry.getKey();
                String count_F_is_f = featureToCount_F_is_f.get(featureWordWithRelation).toString();
                String count_f_with_l = entry.getValue().toString();

                newValueToWrite += featureWordWithRelation + "/" + count_f_with_l + "/" + count_F_is_f + " ";
                // we add every feature to the value in this type:
                //word-relation/count(f,l)/count(F=f)
            }
            newValueToWrite = newValueToWrite.substring(0, newValueToWrite.length() - 1);
            context.write(new Text(headWord) , new Text(newValueToWrite + "\t" + count_L_is_l));

        }
    }

    public static class PartitionerClass3 extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.toString().hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        Configuration conf = new Configuration();
        //timeout configuration 
        conf.set("mapreduce.task.timeout", "14400000"); // 4 hours in milliseconds
        conf.set("mapreduce.reduce.memory.mb", "6144");  // Increase reducer memory
        conf.set("mapreduce.reduce.java.opts", "-Xmx6g"); // Increase JVM heap
        Job job = Job.getInstance(conf, "Step3");
        job.setJarByClass(Step3.class);
        job.setMapperClass(MapperClass3.class);
        job.setPartitionerClass(PartitionerClass3.class);
        job.setReducerClass(ReducerClass3.class);
        job.setNumReduceTasks(9);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String bucketName = "mori-verabi"; // Your S3 bucket name
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://" + bucketName + "/output/step2"));
        TextOutputFormat.setOutputPath(job, new Path("s3://" + bucketName + "/output/step3"));
        
        boolean success = job.waitForCompletion(true);
        
        if(success) {  
            // Retrieve the counter values for COUNT_F and COUNT_L
            long countF = job.getCounters().findCounter(MapperClass3.Counters.COUNT_F).getValue();
            long countL = job.getCounters().findCounter(MapperClass3.Counters.COUNT_L).getValue();

            // Define the S3 paths for the counter files
            String s3PathF = "s3://" + bucketName + "/counters/count_F.txt";
            String s3PathL = "s3://" + bucketName + "/counters/count_L.txt";
            
            // Get the FileSystem instances for the S3 paths
            FileSystem fs_F = FileSystem.get(URI.create(s3PathF), conf);
            FileSystem fs_L = FileSystem.get(URI.create(s3PathL), conf);
            
            // Write the counter values to the files in S3
            Path pathF = new Path(s3PathF);
            Path pathL = new Path(s3PathL);
            
            try (FSDataOutputStream outF = fs_F.create(pathF)) {
            outF.writeBytes("countF =" + countF);
            }
            try(FSDataOutputStream outL = fs_L.create(pathL)) {
            outL.writeBytes("countL =" + countL);
            }
        }

        System.exit(success ? 0 : 1);
    }
}