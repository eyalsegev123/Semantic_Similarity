
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class ConvertToARFF {

    public static String stem(String word) {
        Stemmer stemmer = new Stemmer();
        stemmer.add(word.toCharArray(), word.length()); // Add the full word
        stemmer.stem(); // Perform stemming
        return stemmer.toString();
    }

    public static void main(String[] args) throws IOException {
        // Paths to input and output files
        String step4OutputDir = "../../../../step4_output/"; // Directory containing part-r-* files
        String goldStandardPath = "../../../../word-relatedness.txt";
        String arffOutputPath = "semantic_similarity.arff";

        // Read gold standard to get the relatedness labels
        Map<String, Boolean> goldStandard = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(goldStandardPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length >= 3) {
                    String word1 = stem(parts[0].trim());
                    String word2 = stem(parts[1].trim());
                    Boolean isRelated = Boolean.parseBoolean(parts[2].trim());

                    // Ensure consistent ordering of word pairs (alphabetically)
                    if (word1.compareTo(word2) > 0) {
                        String temp = word1;
                        word1 = word2;
                        word2 = temp;
                    }

                    String key = word1 + "\t" + word2;
                    goldStandard.put(key, isRelated);
                }
            }
        }

        // Write the ARFF file
        try (PrintWriter writer = new PrintWriter(new FileWriter(arffOutputPath))) {
            // Write ARFF header
            writer.println("@RELATION word_relatedness");
            writer.println();

            // Write attribute definitions - 24 similarity measures plus the class
            for (int i = 1; i <= 24; i++) {
                writer.println("@ATTRIBUTE measure_" + i + " NUMERIC");
            }
            writer.println("@ATTRIBUTE class {TRUE,FALSE}");
            writer.println();

            // Write data section header
            writer.println("@DATA");

            // Get all part-r-* files from the directory
            File dir = new File(step4OutputDir);
            File[] partFiles = dir.listFiles((d, name) -> name.startsWith("part-r-"));

            if (partFiles == null || partFiles.length == 0) {
                System.err.println("No part-r-* files found in " + step4OutputDir);
                return;
            }

            System.out.println("Found " + partFiles.length + " part files to process");

            // Process each part file
            int totalPairs = 0;
            int matchedPairs = 0;

            for (File partFile : partFiles) {
                System.out.println("Processing file: " + partFile.getName());

                try (BufferedReader reader = new BufferedReader(new FileReader(partFile))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length >= 3) {
                            String wordPair = parts[0] + "\t" + parts[1];
                            totalPairs++;
                            String similarityScores = parts[2];
                            Boolean isRelated = goldStandard.get(wordPair);

                            if (isRelated != null) {
                                matchedPairs++;
                                // Format the similarity scores for ARFF
                                String[] scores = similarityScores.split("/");
                                StringBuilder dataRow = new StringBuilder();

                                // Add each score to the data row
                                for (int i = 0; i < Math.min(scores.length, 24); i++) {
                                    if (i > 0) {
                                        dataRow.append(",");
                                    }
                                    String score = scores[i].trim();
                                    dataRow.append(score);
                                }

                                // Fill in missing scores if any
                                if (scores.length < 24) {
                                    for (int i = scores.length; i < 24; i++) {
                                        dataRow.append(",0.0");
                                    }
                                }

                                // Add the class label
                                dataRow.append(",").append(isRelated ? "TRUE" : "FALSE");

                                // Write the complete data row
                                writer.println(dataRow.toString());
                            }
                        }
                    }
                }
            }

            System.out.println("Total word pairs found: " + totalPairs);
            System.out.println("Matched with gold standard: " + matchedPairs);
        }

        System.out.println("ARFF file created successfully: " + arffOutputPath);
    }
}
