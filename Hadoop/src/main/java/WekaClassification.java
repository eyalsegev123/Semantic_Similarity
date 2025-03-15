import weka.classifiers.Evaluation;
import weka.classifiers.meta.FilteredClassifier;
import weka.classifiers.trees.J48;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;
import weka.filters.unsupervised.attribute.ReplaceMissingValues;
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
        
        // Create StringBuilder to hold results
        StringBuilder results = new StringBuilder();
        results.append("Classifier,Precision,Recall,F1-Score\n");
        
        // Create J48 decision tree classifier
        J48 j48 = new J48();
        
        // Optional: Configure J48 parameters
        // j48.setConfidenceFactor(0.25f);  // Default is 0.25 (lower values = more pruning)
        // j48.setMinNumObj(2);             // Minimum number of instances per leaf
        
        // Handle missing values using ReplaceMissingValues filter
        FilteredClassifier filteredClassifier = new FilteredClassifier();
        filteredClassifier.setClassifier(j48);
        
        ReplaceMissingValues replaceMissingFilter = new ReplaceMissingValues();
        filteredClassifier.setFilter(replaceMissingFilter);
        
        // Perform 10-fold cross-validation
        Evaluation eval = new Evaluation(data);
        eval.crossValidateModel(filteredClassifier, data, 10, new Random(1));
        
        // Calculate metrics
        double precision = eval.precision(0); // Class index 0 is TRUE
        double recall = eval.recall(0);
        double f1 = (precision + recall > 0) ? 
            (2 * precision * recall) / (precision + recall) : 0;
        
        // Add results to output
        results.append(String.format("Decision Tree (J48),%.4f,%.4f,%.4f\n", 
            precision, recall, f1));
        
        // Print detailed results
        System.out.println("\n=== Detailed Evaluation for Decision Tree (J48) ===");
        System.out.println(eval.toSummaryString());
        System.out.println(eval.toClassDetailsString());
        System.out.println(eval.toMatrixString());
    }
}