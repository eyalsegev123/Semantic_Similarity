import weka.attributeSelection.AttributeSelection;
import weka.attributeSelection.InfoGainAttributeEval;
import weka.attributeSelection.Ranker;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.ReplaceMissingValues;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class FeatureAnalysis {
    
    public static void main(String[] args) throws Exception {
        // Load the dataset
        String arffFilePath = "semantic_similarity.arff";
        DataSource source = new DataSource(arffFilePath);
        Instances data = source.getDataSet();
        
        // Set class index to the last attribute
        if (data.classIndex() == -1) {
            data.setClassIndex(data.numAttributes() - 1);
        }
        
        // Analyze dataset
        analyzeDataset(data);
        
        // Handle missing values
        ReplaceMissingValues replaceMissingFilter = new ReplaceMissingValues();
        replaceMissingFilter.setInputFormat(data);
        Instances processedData = Filter.useFilter(data, replaceMissingFilter);
        
        // Save the processed dataset with missing values handled
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("semantic_similarity_processed.arff"))) {
            writer.write(processedData.toString());
        }
        
        System.out.println("\nProcessed dataset (with missing values handled) saved to semantic_similarity_processed.arff");
        
        // Perform feature ranking using Information Gain (but keep all features)
        AttributeSelection attSelection = new AttributeSelection();
        InfoGainAttributeEval eval = new InfoGainAttributeEval();
        Ranker search = new Ranker();
        search.setGenerateRanking(true);
        attSelection.setEvaluator(eval);
        attSelection.setSearch(search);
        attSelection.SelectAttributes(processedData);
        
        // Get ranking results
        double[][] rankedFeatures = attSelection.rankedAttributes();
        
        // Print and save feature ranking
        System.out.println("\n=== Feature Ranking (Information Gain) ===");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("feature_ranking.csv"))) {
            writer.write("Rank,Feature,InfoGain\n");
            
            for (int i = 0; i < rankedFeatures.length; i++) {
                int featureIndex = (int) rankedFeatures[i][0];
                double score = rankedFeatures[i][1];
                String featureName = processedData.attribute(featureIndex).name();
                
                System.out.println((i+1) + ". " + featureName + ": " + score);
                writer.write((i+1) + "," + featureName + "," + score + "\n");
            }
        }
        
        System.out.println("\nFeature ranking saved to feature_ranking.csv");
        System.out.println("All 24 features will be used for classification.");
    }
    
    private static void analyzeDataset(Instances data) {
        System.out.println("\n=== Dataset Analysis ===");
        System.out.println("Number of instances: " + data.numInstances());
        System.out.println("Number of attributes: " + data.numAttributes());
        
        // Check for missing values in each attribute
        System.out.println("\nMissing values analysis:");
        for (int i = 0; i < data.numAttributes(); i++) {
            int missingCount = 0;
            for (int j = 0; j < data.numInstances(); j++) {
                if (data.instance(j).isMissing(i)) {
                    missingCount++;
                }
            }
            
            if (missingCount > 0) {
                System.out.println("Attribute " + data.attribute(i).name() + 
                                  " has " + missingCount + " missing values (" + 
                                  String.format("%.2f%%", (missingCount * 100.0 / data.numInstances())) + ")");
            }
        }
        
        // Class distribution
        int[] classCounts = new int[data.attribute(data.classIndex()).numValues()];
        for (int i = 0; i < data.numInstances(); i++) {
            classCounts[(int) data.instance(i).classValue()]++;
        }
        
        System.out.println("\nClass distribution:");
        for (int i = 0; i < classCounts.length; i++) {
            System.out.println(data.attribute(data.classIndex()).value(i) + ": " + 
                              classCounts[i] + " (" + 
                              String.format("%.2f%%", (classCounts[i] * 100.0 / data.numInstances())) + ")");
        }
        
        // Basic statistics for each feature
        System.out.println("\nFeature statistics:");
        for (int i = 0; i < data.numAttributes() - 1; i++) { // Skip the class attribute
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            double sum = 0;
            int count = 0;
            
            for (int j = 0; j < data.numInstances(); j++) {
                if (!data.instance(j).isMissing(i)) {
                    double value = data.instance(j).value(i);
                    min = Math.min(min, value);
                    max = Math.max(max, value);
                    sum += value;
                    count++;
                }
            }
            
            double mean = (count > 0) ? sum / count : 0;
            
            System.out.println(data.attribute(i).name() + 
                              " - Min: " + String.format("%.4f", min) + 
                              ", Max: " + String.format("%.4f", max) + 
                              ", Mean: " + String.format("%.4f", mean));
        }
    }
}