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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;   


public class Step3past {
    
    public static class MapperClass3 extends Mapper<LongWritable, Text, Text, Text> {
        //ngram format from Google Books:
        //ngram TAB year TAB match_count TAB page_count TAB volume_count NEWLINE
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] fields = value.toString().split("\t"); //The key and all the values
            String ngram = fields[0]; //The key
            String[] ngramWords = ngram.split(" "); // Split by space
            String valuesInString = "";
            
            if(ngramWords.length != 1){                
                return;
            }

            for(int i = 1; i < fields.length; i++) {
                if(i == fields.length - 1)
                    valuesInString += fields[i];
                else
                    valuesInString += fields[i] + "\t";
            }

            context.write(new Text(ngram), new Text(valuesInString));
        }
        
    }


    public static class ReducerClass3 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {

            String[] words = key.toString().split(" "); 

            //Word length should be 1
            if(words.length != 1){   
                return;
            }
            
             //Length is 1 as it should be
            String original1GramValue = "null";
            List<Text> notOriginalValues = new LinkedList<>();
            for(Text value : values) {
                if(value.toString().split("\t").length == 1) 
                    original1GramValue = value.toString(); 
                else
                    notOriginalValues.add(value);
            }   

            StringBuilder newValueOf1gram = new StringBuilder();
            for(Text value : notOriginalValues){
                newValueOf1gram.setLength(0);
                newValueOf1gram.append(original1GramValue + "\t" + value.toString());
            
                String[] threeGramWithNumberArray = value.toString().split("\t");
                String threeGramWithNumber = "null";
                if(threeGramWithNumberArray.length < 5)
                    return;
                else
                    threeGramWithNumber = threeGramWithNumberArray[4];
                    
                int indexOfColon = threeGramWithNumber.lastIndexOf(":");
                String threeGram = threeGramWithNumber.substring(0, indexOfColon);
                context.write(new Text(threeGram) , new Text(newValueOf1gram.toString()));
            }        
        }        
    }            

    public static class PartitionerClass3 extends Partitioner<Text, Text> { //Get partition wasnt done today
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String ngram = key.toString();
            String firstWord = ngram.split(" ")[0];
            return Math.abs(firstWord.hashCode() % numPartitions);
        }
    }

     public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        String bucketName = "hashem-itbarach";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step3 - Second Join");
        job.setJarByClass(Step3.class);
        job.setMapperClass(MapperClass3.class);
        job.setPartitionerClass(PartitionerClass3.class);
        // job.setCombinerClass(ReducerClass3.class);
        job.setReducerClass(ReducerClass3.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // For n_grams S3 files.
        // Note: This is English version and you should change the path to the relevant one
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://" + bucketName + "/output/step2"));
        TextOutputFormat.setOutputPath(job, new Path("s3://" + bucketName + "/output/step3"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}