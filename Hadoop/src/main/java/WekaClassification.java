import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.functions.SMO;
import weka.classifiers.lazy.IBk;
import weka.classifiers.meta.FilteredClassifier;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;
import weka.core.Utils;
import weka.core.converters.ConverterUtils.DataSource;
import weka.filters.unsupervised.attribute.Remove;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Random;

public class WekaClassification {
    
    public static void main(String[] args) throws Exception {
        // Load the dataset
        String arffFilePath = "semantic_similarity.arff";
        DataSource source = new DataSource(arffFilePath);
        Instances data = source.getDataSet();
        
        // Set class index to the last attribute
        if (data.classIndex() == -1) {
            data.setClassIndex(data.numAttributes() - 1);
        }
        
        // Create instances to hold results
        StringBuilder results = new StringBuilder();
        results.append("Classifier,Precision,Recall,F1-Score\n");
        
        // Array of classifiers to test
        Classifier[] classifiers = {
            new J48(), // Decision Tree
            new RandomForest(), // Random Forest
            new SMO(), // Support Vector Machine
            new IBk() // K-Nearest Neighbors
        };
        
        String[] classifierNames = {
            "Decision Tree (J48)",
            "Random Forest",
            "Support Vector Machine (SMO)",
            "K-Nearest Neighbors (IBk)"
        };
        
        // Train and evaluate each classifier
        for (int i = 0; i < classifiers.length; i++) {
            Classifier classifier = classifiers[i];
            
            // Handle missing values (NaN) by removing problematic features if needed
            FilteredClassifier filteredClassifier = new FilteredClassifier();
            filteredClassifier.setClassifier(classifier);
            
            // Create a filter to remove problematic features if needed
            // You might want to adjust this based on your analysis of which features have NaN values
            Remove removeFilter = new Remove();
            removeFilter.setAttributeIndices(""); // No attributes to remove by default
            filteredClassifier.setFilter(removeFilter);
            
            // Perform 10-fold cross-validation
            Evaluation eval = new Evaluation(data);
            eval.crossValidateModel(filteredClassifier, data, 10, new Random(1));
            
            // Calculate metrics
            double precision = eval.precision(0); // Class index 0 is TRUE
            double recall = eval.recall(0);
            double f1 = 2 * (precision * recall) / (precision + recall);
            
            // Add results to output
            results.append(String.format("%s,%.4f,%.4f,%.4f\n", 
                classifierNames[i], precision, recall, f1));
            
            // Print detailed results for this classifier
            System.out.println("\n=== Detailed Evaluation for " + classifierNames[i] + " ===");
            System.out.println(eval.toSummaryString());
            System.out.println(eval.toClassDetailsString());
            System.out.println(eval.toMatrixString());
        }
        
        // Save results to file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("classification_results.csv"))) {
            writer.write(results.toString());
        }
        
        System.out.println("\nClassification results saved to classification_results.csv");
    }
    
    // Optional: Method to analyze the dataset for missing values and other statistics
    private static void analyzeDataset(Instances data) {
        System.out.println("\n=== Dataset Analysis ===");
        System.out.println("Number of instances: " + data.numInstances());
        System.out.println("Number of attributes: " + data.numAttributes());
        
        // Check for missing values in each attribute
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
    }
}
