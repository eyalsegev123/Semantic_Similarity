import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

//caculating the 24 coordinates
public class Step4 {

    public static class MapperClass4 extends Mapper<LongWritable, Text, Text, Text> {

        private HashMap<String, HashSet<String>> goldenPairs = new HashMap<>(); // maps every word in the word-relatedness.txt to a set of the words it comes with in pairs
        long countF;
        long countL;

        public String stem(String word) {
            Stemmer stemmer = new Stemmer();
            stemmer.add(word.toCharArray(), word.length()); // Add the full word
            stemmer.stem(); // Perform stemming
            return stemmer.toString();
        }

        public void setup(Context context) throws IOException, InterruptedException {
            countF = context.getConfiguration().getLong("countF", 0);
            countL = context.getConfiguration().getLong("countL", 0);
            // Configure AWS client using instance profile credentials (recommended when
            // running on AWS infrastructure)
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion("us-east-1") // Specify your bucket region
                    .build();

            String bucketName = "teacherandrabi"; // Your S3 bucket name
            String key = "word-relatedness.txt"; // S3 object key for the stopwords file

            try {
                S3Object s3object = s3Client.getObject(bucketName, key);
                try (S3ObjectInputStream inputStream = s3object.getObjectContent(); BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] fields = line.split("\t");
                        String word1 = stem(fields[0].trim());
                        String word2 = stem(fields[1].trim());

                        if (goldenPairs.get(word1) != null) {
                            goldenPairs.get(word1).add(word2);
                        } else {
                            goldenPairs.put(word1, new HashSet<String>());
                            goldenPairs.get(word1).add(word2);
                        }

                        if (goldenPairs.get(word2) != null) {
                            goldenPairs.get(word2).add(word1);
                        } else {
                            goldenPairs.put(word2, new HashSet<String>());
                            goldenPairs.get(word2).add(word1);
                        }

                    }
                }
            } catch (Exception e) {
                // Handle exceptions properly in a production scenario
                e.printStackTrace();
            }
        }

        //We get here the output of step 3
        //Key: number of line
        //Value: <headWord TAB feature1-POS/count_f_is_F/count_f_with_l .... featureN-POS/count_f_with_l/count_F_is_f   TAB  count_L_is_l>
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t"); // Split by tab
            if (fields.length == 0) {
                return;
            }

            String headWord = fields[0];
            if (goldenPairs.get(headWord) == null) {
                return;
            }
            String[] featuresArray = fields[1].split(" ");
            long count_L_is_l = Long.parseLong(fields[2]);

            HashMap<String, Double[]> featuresByCount = new HashMap<>();

            for (String feature : featuresArray) {
                String[] featureFields = feature.split("/");
                String featureWordRelation = featureFields[0];
                double count_f_With_l = Double.parseDouble(featureFields[1]);
                double count_F_is_f = Double.parseDouble(featureFields[2]);
                Double[] arrayOfCounts = {count_f_With_l, count_F_is_f};
                featuresByCount.put(featureWordRelation, arrayOfCounts);
            }

            //Now we will need to calculate the values of each cordinate by all the methods (5,6,7,8)
            HashMap<String, Double> measures_by_method_5 = measures_by_method_5(featuresByCount);
            HashMap<String, Double> measures_by_method_6 = measures_by_method_6(measures_by_method_5);
            HashMap<String, Double> measures_by_method_7 = measures_by_method_7(featuresByCount, measures_by_method_6);
            HashMap<String, Double> measures_by_method_8 = measures_by_method_8(featuresByCount, count_L_is_l);

            HashSet<String> relatedWords = goldenPairs.get(headWord);
            for (String relatedWord : relatedWords) {
                String[] key_value = makePairToReducer(headWord, relatedWord, measures_by_method_5, measures_by_method_6, measures_by_method_7, measures_by_method_8);
                context.write(new Text(key_value[0]), new Text(key_value[1]));
            }
        }

        protected String[] makePairToReducer(String word, String relatedWord, HashMap<String, Double> m5,
                HashMap<String, Double> m6, HashMap<String, Double> m7, HashMap<String, Double> m8) {
            String firstWord = word;
            String secondWord = relatedWord;
            if (firstWord.compareTo(secondWord) > 0) { // firstWord is lexicofraphically bigger than secondWord   
                String temp = firstWord;
                firstWord = secondWord;
                secondWord = temp;
            }
            String keyToWrite = firstWord + "\t" + secondWord; //we want every pair to get the same reducer so the key will be arranged alphabeticly
            String valueToWrite = "";
            HashMap<String, String> featuresByMeasures = new HashMap<>();

            for (Map.Entry<String, Double> entry : m5.entrySet()) {
                String formerValueInHashMap = featuresByMeasures.getOrDefault(entry.getKey(), "");
                featuresByMeasures.put(entry.getKey(), formerValueInHashMap + entry.getValue().toString() + "/");
            }
            for (Map.Entry<String, Double> entry : m6.entrySet()) {
                String formerValueInHashMap = featuresByMeasures.getOrDefault(entry.getKey(), "");
                featuresByMeasures.put(entry.getKey(), formerValueInHashMap + entry.getValue().toString() + "/");
            }
            for (Map.Entry<String, Double> entry : m7.entrySet()) {
                String formerValueInHashMap = featuresByMeasures.getOrDefault(entry.getKey(), "");
                featuresByMeasures.put(entry.getKey(), formerValueInHashMap + entry.getValue().toString() + "/");
            }
            for (Map.Entry<String, Double> entry : m8.entrySet()) {
                String formerValueInHashMap = featuresByMeasures.getOrDefault(entry.getKey(), "");
                featuresByMeasures.put(entry.getKey(), formerValueInHashMap + entry.getValue().toString());
            }

            // now in featuresByMeasures we have every feature of word like this:
            // key: feature-relation value: m5/m6/m7/m8
            for (Map.Entry<String, String> entry : featuresByMeasures.entrySet()) {
                valueToWrite += entry.getKey() + " " + entry.getValue() + "\t";
            }

            String[] ans = {keyToWrite, valueToWrite};
            return ans;

        }

        protected HashMap<String, Double> measures_by_method_5(HashMap<String, Double[]> featuresByCount) {
            HashMap<String, Double> measures_by_method_5 = new HashMap<>();
            for (Map.Entry<String, Double[]> entry : featuresByCount.entrySet()) {
                measures_by_method_5.put(entry.getKey(), entry.getValue()[0] );
            }
            return measures_by_method_5;
        }

        protected HashMap<String, Double> measures_by_method_6(HashMap<String, Double> measures_by_method_5) {
            HashMap<String, Double> measures_by_method_6 = new HashMap<>();
            for (Map.Entry<String, Double> entry : measures_by_method_5.entrySet()) {
                measures_by_method_6.put(entry.getKey(), entry.getValue() / this.countL);
            }
            return measures_by_method_6;
        }

        protected HashMap<String, Double> measures_by_method_7(HashMap<String, Double[]> featuresByCount, HashMap<String, Double> measures_by_method_6) {
            HashMap<String, Double> measures_by_method_7 = new HashMap<>();
            for (Map.Entry<String, Double> entry : measures_by_method_6.entrySet()) {
                Double P_of_f = featuresByCount.get(entry.getKey())[1] / (double) this.countF;
                measures_by_method_7.put(entry.getKey(), Math.log((entry.getValue() / P_of_f)) / Math.log(2));
            }
            return measures_by_method_7;
        }

        protected HashMap<String, Double> measures_by_method_8(HashMap<String, Double[]> featuresByCount, long count_L_is_l) {
            HashMap<String, Double> measures_by_method_8 = new HashMap<>();
            for (Map.Entry<String, Double[]> entry : featuresByCount.entrySet()) {
                Double P_of_f_With_l = entry.getValue()[0] / this.countL;
                Double P_of_f = entry.getValue()[1] / this.countF;
                Double P_of_l = (double) count_L_is_l / (double) this.countL;
                Double measure = (P_of_f_With_l - P_of_f * P_of_l) / Math.sqrt(P_of_l * P_of_f);
                measures_by_method_8.put(entry.getKey(), measure);
            }
            return measures_by_method_8;
        }
    }

    public static class ReducerClass4 extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            HashSet<String> valuesSet = new HashSet<>();
            for (Text value : values) {
                valuesSet.add(value.toString());
            }

            if (valuesSet.size() != 2) {
                return;
            }

            //we want to get the two values and check that theyre not null
            String valuesString[] = valuesSet.toArray(new String[2]);
            for (String value : valuesString) {
                if (value == null) {
                    return;
                }
            }

            int numberOfFeaturesInFirst = valuesString[0].split("\t").length;
            int numberOfFeaturesInSecond = valuesString[1].split("\t").length;

            if (numberOfFeaturesInFirst == 0 || numberOfFeaturesInSecond == 0) {
                return;
            }

            LinkedList<String>[] vectors = new LinkedList[2];
            vectors[0] = new LinkedList<>();
            vectors[1] = new LinkedList<>();
            insertCommonAndNotCommonFeatures(vectors, valuesString);

            // Convert LinkedList to String array
            String[][] vectors_array = new String[2][];
            vectors_array[0] = vectors[0].toArray(new String[0]);
            vectors_array[1] = vectors[1].toArray(new String[0]);

            //Here we get the distance between each l1 and l2 
            double[] dist_by_method_9 = dist_by_method_9(vectors_array);
            double[] dist_by_method_10 = dist_by_method_10(vectors_array);
            double[] dist_by_method_11 = dist_by_method_11(vectors_array);
            double[] dist_by_method_13 = dist_by_method_13(vectors_array);
            double[] dist_by_method_15 = dist_by_method_15(vectors_array);
            double[] dist_by_method_17 = dist_by_method_17(context , vectors_array);

            String final_24_vector = "";
            for (double dist : dist_by_method_9) {
                dist = Math.round(dist * 10000.0) / 10000.0;
                final_24_vector += dist + "/";
            }

            for (double dist : dist_by_method_10) {
                dist = Math.round(dist * 10000.0) / 10000.0;
                final_24_vector += dist + "/";
            }

            for (double dist : dist_by_method_11) {
                dist = Math.round(dist * 10000.0) / 10000.0;
                final_24_vector += dist + "/";
            }

            for (double dist : dist_by_method_13) {
                dist = Math.round(dist * 10000.0) / 10000.0;
                final_24_vector += dist + "/";
            }

            for (double dist : dist_by_method_15) {
                dist = Math.round(dist * 10000.0) / 10000.0;
                final_24_vector += dist + "/";
            }

            for (double dist : dist_by_method_17) {
                dist = Math.round(dist * 10000.0) / 10000.0;
                final_24_vector += dist + "/";
            }

            context.write(key, new Text(final_24_vector));
            // String  valueString = "";
            // int countValues = 0; 
            // for(Text value : values){
            //     valueString += value.toString() + "\t";
            //     countValues++;
            // }
            // context.write(new Text(key.toString() + " values: " + countValues), new Text(valueString));

        }

        protected void insertCommonAndNotCommonFeatures(LinkedList<String>[] vectors, String[] values) {

            //we will start with the common features
            HashMap<String, String> featuresOfFirstWord = new HashMap<>();
            HashMap<String, String> featuresOfSecondWord = new HashMap<>();
            String[] word1_features_array = values[0].split("\t");
            String[] word2_features_array = values[1].split("\t");

            //Extract the features of each word
            for (String feature : word1_features_array) {
                String featureWordAndRelation = feature.split(" ")[0];
                String featureMeasures = feature.split(" ")[1];
                featuresOfFirstWord.put(featureWordAndRelation, featureMeasures);
            }
            for (String feature : word2_features_array) {
                String featureWordAndRelation = feature.split(" ")[0];
                String featureMeasures = feature.split(" ")[1];
                featuresOfSecondWord.put(featureWordAndRelation, featureMeasures);
            }

            //Now we will insert the common features
            for (Map.Entry<String, String> entry : featuresOfFirstWord.entrySet()) {
                if (featuresOfSecondWord.get(entry.getKey()) != null) {
                    vectors[0].add(entry.getValue());
                    vectors[1].add(featuresOfSecondWord.get(entry.getKey()));
                }
            }

            //Now we will insert the NOT common features
            for (Map.Entry<String, String> entry : featuresOfFirstWord.entrySet()) {
                if (featuresOfSecondWord.get(entry.getKey()) == null) {
                    vectors[0].add(entry.getValue());
                    vectors[1].add("0/0/0/0");
                }
            }
            for (Map.Entry<String, String> entry : featuresOfSecondWord.entrySet()) {
                if (featuresOfFirstWord.get(entry.getKey()) == null) {
                    vectors[0].add("0/0/0/0");
                    vectors[1].add(entry.getValue());
                }
            }
        }

        protected double[] dist_by_method_9(String[][] vectors) {
            double[] ans = new double[4];
            for (int i = 0; i < ans.length; i++) {
                double sum = 0.0;
                for (int j = 0; j < vectors[0].length; j++) {
                    double firstWordCoordinate = Double.parseDouble(vectors[0][j].split("/")[i]); //we want the i-th measure method
                    double secondWordCoordinate = Double.parseDouble(vectors[1][j].split("/")[i]); //we want the i-th measure method
                    sum += Math.abs(firstWordCoordinate - secondWordCoordinate);
                }
                ans[i] = sum;
            }
            return ans;
        }

        protected double[] dist_by_method_10(String[][] vectors) {
            double[] ans = new double[4];
            for (int i = 0; i < ans.length; i++) {
                double sum = 0.0;
                for (int j = 0; j < vectors[0].length; j++) {
                    double firstWordCoordinate = Double.parseDouble(vectors[0][j].split("/")[i]); //we want the i-th measure method
                    double secondWordCoordinate = Double.parseDouble(vectors[1][j].split("/")[i]); //we want the i-th measure method
                    double squaredDifference = Math.pow((firstWordCoordinate - secondWordCoordinate), 2);
                    sum += squaredDifference;
                }
                ans[i] = Math.sqrt(sum);
            }
            return ans;
        }

        protected double[] dist_by_method_11(String[][] vectors) {
            double[] ans = new double[4];
            for (int i = 0; i < ans.length; i++) {
                double sumOfProducts = 0.0;
                double sumOfSquaredVector1 = 0.0;
                double sumOfSquaredVector2 = 0.0;
                for (int j = 0; j < vectors[0].length; j++) {
                    double firstWordCoordinate = Double.parseDouble(vectors[0][j].split("/")[i]); //we want the i-th measure method
                    double secondWordCoordinate = Double.parseDouble(vectors[1][j].split("/")[i]); //we want the i-th measure method
                    sumOfProducts += firstWordCoordinate * secondWordCoordinate;
                    sumOfSquaredVector1 += Math.pow(firstWordCoordinate, 2);
                    sumOfSquaredVector2 += Math.pow(secondWordCoordinate, 2);
                }
                ans[i] = sumOfProducts / (Math.sqrt(sumOfSquaredVector1) * Math.sqrt(sumOfSquaredVector2));
            }
            return ans;
        }

        protected double[] dist_by_method_13(String[][] vectors) {
            double[] ans = new double[4];
            for (int i = 0; i < ans.length; i++) {
                double sumOfMin = 0.0;
                double sumOfMax = 0.0;
                for (int j = 0; j < vectors[0].length; j++) {
                    double firstWordCoordinate = Double.parseDouble(vectors[0][j].split("/")[i]); //we want the i-th measure method
                    double secondWordCoordinate = Double.parseDouble(vectors[1][j].split("/")[i]); //we want the i-th measure method
                    sumOfMin += Math.min(firstWordCoordinate, secondWordCoordinate);
                    sumOfMax += Math.max(firstWordCoordinate, secondWordCoordinate);
                }
                ans[i] = sumOfMin / sumOfMax;
            }
            return ans;
        }

        protected double[] dist_by_method_15(String[][] vectors) {
            double[] ans = new double[4];
            for (int i = 0; i < ans.length; i++) {
                double sumOfMin = 0.0;
                double sumOfVectorsCoordinates = 0.0;
                for (int j = 0; j < vectors[0].length; j++) {
                    double firstWordCoordinate = Double.parseDouble(vectors[0][j].split("/")[i]); //we want the i-th measure method
                    double secondWordCoordinate = Double.parseDouble(vectors[1][j].split("/")[i]); //we want the i-th measure method
                    sumOfMin += Math.min(firstWordCoordinate, secondWordCoordinate);
                    sumOfVectorsCoordinates += firstWordCoordinate + secondWordCoordinate;
                }
                ans[i] = (2 * sumOfMin) / sumOfVectorsCoordinates;
            }
            return ans;
        }

        protected double[] dist_by_method_17(Context context , String[][] vectors) {

            String[] l1 = vectors[0];
            String[] l2 = vectors[1];
            Double[][] M = new Double[l1.length][4];

            // DEBUG
            // String l1_str = "";
            // String l2_str = "";
            // for(String s : l1)
            //     l1_str += s;
            // for(String s : l2)
            //     l2_str += s;
            // try {
            //     context.write(new Text("[DEBUG] - ") , new Text("DIST BY 17: l1- " + l1_str + " l2 -" + l2_str));
            // }
            // catch(Exception e) {
            //     System.out.println("ahla");
            // }
            //DEBUG
            

            for (int j = 0; j < l1.length; j++) {
                String[] l1Measures = l1[j].split("/");
                String[] l2Measures = l2[j].split("/");
                for (int k = 0; k < 4; k++) {
                    Double l1Cord = Double.parseDouble(l1Measures[k]);
                    Double l2Cord = Double.parseDouble(l2Measures[k]);
                    M[j][k] = (l1Cord + l2Cord) / 2;
                }
            }

            double[] ans = new double[4];
            for (int i = 0; i < ans.length; i++) {
                ans[i] = (KL_divergence(l1, M, i) + KL_divergence(l2, M, i)) / 2;

                // //DEBUG
                // try {
                //     context.write(new Text("[DEBUG] - ") , new Text("DIST BY 17: KL_div_l1: " + KL_divergence(l1, M, i)));
                //     context.write(new Text("[DEBUG] - ") , new Text("DIST BY 17: KL_div_l2: " + KL_divergence(l2, M, i)));
                //     context.write(new Text("[DEBUG] - ") , new Text("DIST BY 17: ans[" + i + "]: " + ans[i]));
                // }
                // catch(Exception e) {
                //     System.out.println("ahla");
                // }
                
            }

            return ans;
        }

        protected double KL_divergence(String[] L, Double[][] M, int coordinate_index) {
            double sum = 0.0;
            for (int i = 0; i < L.length; i++) {
                double L_Coordinate = Double.parseDouble(L[i].split("/")[coordinate_index]);
                double M_Coordinate = M[i][coordinate_index];
                if(L_Coordinate == 0.0) L_Coordinate = 0.0001;
                if(M_Coordinate == 0.0) M_Coordinate = 0.0001;
                sum += L_Coordinate * ((Math.log(L_Coordinate / M_Coordinate)) / Math.log(2));
            }
            return sum;
        }

    }

    public static class PartitionerClass4 extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.toString().hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {

        System.out.println("[DEBUG] STEP 4 started!");
        String bucketName = "teacherandrabi";

        //Step 1: Initialize Configuration
        Configuration conf = new Configuration();
        //timeout configuration 
        conf.set("mapreduce.task.timeout", "14400000"); // 4 hours in milliseconds
        conf.set("mapreduce.reduce.memory.mb", "6144");  // Increase reducer memory
        conf.set("mapreduce.reduce.java.opts", "-Xmx6g"); // Increase JVM heap

        // Step 2: Retrieve the counter value from S3
        String counterF_FilePath = "s3://" + bucketName + "/counters/count_F.txt";
        String counterL_FilePath = "s3://" + bucketName + "/counters/count_L.txt";

        // Step 3: Read the counter value from the file in S3
        FileSystem fs = FileSystem.get(new URI("s3://" + bucketName), conf);
        Path counterPath_F = new Path(counterF_FilePath);
        Path counterPath_L = new Path(counterL_FilePath);

        FSDataInputStream in = fs.open(counterPath_F);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line;
        long countF = 0, countL = 0;

        // Parse the counter value from the file
        while ((line = br.readLine()) != null) {
            if (line.contains("=")) { // Ensure it has the expected format
                String[] parts = line.split("=");
                if (parts.length == 2) {
                    countF = Long.parseLong(parts[1].trim());
                }
            }
        }

        in = fs.open(counterPath_L);
        br = new BufferedReader(new InputStreamReader(in));

        // Parse the counter value from the file
        while ((line = br.readLine()) != null) {
            if (line.contains("=")) { // Ensure it has the expected format
                String[] parts = line.split("=");
                if (parts.length == 2) {
                    countL = Long.parseLong(parts[1].trim());
                }
            }
        }

        br.close();
        in.close();

        //Set the counters in the Hadoop environment
        conf.setLong("countF", countF);
        conf.setLong("countL", countL);

        Job job = Job.getInstance(conf, "Step4");
        job.setJarByClass(Step4.class);
        job.setMapperClass(MapperClass4.class);
        job.setPartitionerClass(PartitionerClass4.class);
        job.setReducerClass(ReducerClass4.class);
        job.setNumReduceTasks(9);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path("s3://" + bucketName + "/output/step3"));
        TextOutputFormat.setOutputPath(job, new Path("s3://" + bucketName + "/output/step4"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
