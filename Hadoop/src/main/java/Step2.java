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

public class Step2 {

    public static class MapperClass2 extends Mapper<LongWritable, Text, Text, Text> {

        // ngram format from Google Books:
        // ngram TAB year TAB match_count TAB page_count TAB volume_count NEWLINE
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t"); // Split by tab
            String ngram = fields[0];
            String[] ngramWords = ngram.split(" "); // Split by space
            String stringValue = "";
            
            for(int i = 1; i < fields.length; i++) {
                if(i == fields.length - 1)
                    stringValue += fields[i];
                else
                    stringValue += fields[i] + "\t";
            }

            if (ngramWords.length == 3) {
                // Extract n-gram and count data
                String twoGram = ngramWords[1] + " " + ngramWords[2]; // "w1 w2"
                // Emit 2-gram
                context.write(new Text(twoGram), new Text(stringValue));
            } 
            else
                context.write(new Text(ngram), new Text(stringValue));
        }

    }

    // yeled : 40 - send as it is
    // yeled tov : 50 - search yeled and send yeled tov yeled
    // yeled nehmad :90
    // yeled tov halah : 60
    // yeled nehmad halah : 70

    public static class ReducerClass2 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String[] ngramWords = key.toString().split(" ");
            boolean h = false;
            if (ngramWords.length == 1) {
                if(ngramWords[0].trim().equals("החיצונית")){
                    h = true;
                }
                String valueOf1gram = "";
                Integer numOfValues = 0;
                for (Text value : values) {
                    if(h)
                        context.write(new Text("[DEBUG]: key --> " + key.toString()) , new Text(numOfValues + "_th value --> " + value.toString()));
                    valueOf1gram += value.toString();
                    numOfValues++;
                }
                context.write(key , new Text(valueOf1gram));
            }

           
            else { // Length is 2
                if (ngramWords.length != 2) {
                    return;
                }
    
                String original2GramValue = "null";
                List<Text> notOriginalValues = new LinkedList<>();
                Integer numOfValues2 = 0;
                for (Text value : values) {
                    if (value.toString().split("\t").length == 2) {
                        original2GramValue = value.toString();
                        numOfValues2++;                    
                    }
                    else 
                        notOriginalValues.add(value);
                }

                StringBuilder newValueOf2gram = new StringBuilder();
                for (Text value : notOriginalValues) {
                    newValueOf2gram.setLength(0);
                    newValueOf2gram.append(original2GramValue + "\t" + value.toString()); // Now the 3gram is in the 5th value. original2gramvalue can be "null"
                    context.write(new Text(ngramWords[1]), new Text(newValueOf2gram.toString()));
                }
            }   
        }
    }

    public static class PartitionerClass2 extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String ngram = key.toString();
            String firstWord = ngram.split(" ")[0];
            return Math.abs(firstWord.hashCode() % numPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        String bucketName = "hashem-itbarach";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step2 - First Join");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass2.class);
        job.setPartitionerClass(PartitionerClass2.class);
        job.setReducerClass(ReducerClass2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // For n_grams S3 files.
        // Note: This is English version and you should change the path to the relevant
        // one
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://" + bucketName + "/output/step1"));
        TextOutputFormat.setOutputPath(job, new Path("s3://" + bucketName + "/output/step2"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}