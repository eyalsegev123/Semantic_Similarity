import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;



public class Step4past {
    public static class MapperClass4 extends Mapper<LongWritable, Text, Text, Text> {
        private long c0;
        
        protected void setup(Context context) throws IOException, InterruptedException {
            // Read c0 from the job configuration passed in Step 4
            c0 = context.getConfiguration().getLong("c0Value", 1L);  // Default value set to 1L if not found
            
            System.out.println("Successfully loaded c0 from job configuration: " + c0);
        }
        
        //ngram format from Google Books:
        //ngram TAB year TAB match_count TAB page_count TAB volume_count NEWLINE
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] fields = value.toString().split("\t"); //The key and all the values
            String ngram = fields[0]; //The key
            String firstWord = ngram.split(" ")[0];

            String[] values = new String[fields.length - 1];
            for(int i = 1; i < fields.length; i++) {
                values[i-1] = fields[i];
            }
            if(values.length < 6)
                return;

            Double[] numbersOfValues = new Double[values.length];
            for(int i = 0; i < numbersOfValues.length; i++) {
                // If the first word of the 3gram is null it's not an error becaue we don't need it to calculate the probability.
                String[] currValue = values[i].split(":");
                if(currValue.length < 2 || ((!(currValue[0].equals(firstWord))) && currValue[1] == "null")) {
                    return;
                }
                numbersOfValues[i] = Double.parseDouble(currValue[1]);               
            }
            
            double N1 = numbersOfValues[0]; //String N1
            double N2 = numbersOfValues[2]; //String N2
            double N3 = numbersOfValues[5]; //String N3
            double C1 = numbersOfValues[1]; //String C1
            double C2 = numbersOfValues[4]; //String C2
            
            double K2 = ((Math.log(N2+1)/Math.log(2)) + 1 ) / (( Math.log(N2+1) / Math.log(2)) + 2); 
            double K3 = ((Math.log(N3+1)/Math.log(2)) + 1 ) / (( Math.log(N3+1) / Math.log(2)) + 2); 
            
            //Calculate the probability
            double probabilty = K3*(N3/C2) + (1-K3)*K2*(N2/C1) + (1-K3)*(1-K2)*((double) N1/c0);
            String prob = String.format("%.5f", probabilty);
            
            // Create a new output key and value
            Text outputKey = new Text(ngram + " " + prob); // Copy the key
            Text outputValue = new Text(""); // Concatenate probability with original data
            
            // Write the key-value pair to the context
            context.write(outputKey, outputValue);
        }
        
    }


    public static class ReducerClass4 extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            Text value = values.iterator().next();
            context.write(key, value);
        }        
    }            

    public static class PartitionerClass4 extends Partitioner<Text, Text> { //Get partition wasnt done today
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] parts = key.toString().split(" ");
            int hash = (parts[0] + parts[1]).hashCode(); // Combine `w1` and `w2`
            return Math.abs(hash) % numPartitions;
        }
    }

    public static class Step4Comparator extends WritableComparator {
        protected Step4Comparator() {
            super(Text.class, true);
        }
    
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            String[] ngram1 = ((Text) w1).toString().split(" ");
             String[] ngram2 = ((Text) w2).toString().split(" ");
            
            // Compare first words
            int cmp = ngram1[0].compareTo(ngram2[0]);
            if (cmp != 0) {
                return cmp;
            }
            // Compare second words
            cmp = ngram1[1].compareTo(ngram2[1]);
            if (cmp != 0) {
                return cmp;
            }

            // Compare probabilities (reverse order for descending)
            // Assuming probabilities are the last element in the array and are parseable as doubles
            double prob1 = Double.parseDouble(ngram1[ngram1.length - 1]);
            double prob2 = Double.parseDouble(ngram2[ngram2.length - 1]);

            return prob1 < prob2 ? 1 : (prob1 > prob2 ? -1 : 0);
            
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 4 started!");

        // Step 1: Initialize Configuration
        Configuration conf = new Configuration();

        // Step 2: Retrieve the counter value from S3
        String bucketName = "hashem-itbarach";
        String counterFilePath = "s3://" + bucketName + "/output/step1/counter_c0.txt";
        FileSystem fs = FileSystem.get(new URI("s3://" + bucketName), conf);
        Path counterPath = new Path(counterFilePath);

        // Read the counter value from the file in S3
        FSDataInputStream in = fs.open(counterPath);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line;
        long c0Value = 0;

        // Parse the counter value from the file
        while ((line = br.readLine()) != null) {
            if (line.contains("Counter C0 Value:")) {
                String[] parts = line.split(":");
                c0Value = Long.parseLong(parts[1].trim());
                System.out.println("Retrieved Counter C0 Value: " + c0Value);
            }
        }

        br.close();
        in.close();

        // Step 3: Configure the Job
        Job job = Job.getInstance(conf, "Step 4 - Processing with c0");
        job.setJarByClass(Step4.class);
        job.setMapperClass(MapperClass4.class);
        job.setPartitionerClass(PartitionerClass4.class);
        job.setSortComparatorClass(Step4Comparator.class);
        job.setReducerClass(ReducerClass4.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Pass the counter value to the job (can be done using JobConf or other methods if needed)
        job.getConfiguration().setLong("c0Value", c0Value);

        // Step 4: Set Input/Output Paths
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://" + bucketName + "/output/step3"));
        TextOutputFormat.setOutputPath(job, new Path("s3://" + bucketName + "/output/step4"));

        // Step 5: Run the Job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
}

     
}