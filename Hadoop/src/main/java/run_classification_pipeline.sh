#!/bin/bash

# Script to run the complete Semantic Similarity classification pipeline

echo "===== Semantic Similarity Classification Pipeline ====="

# Step 2: Compile Java code
echo "Compiling Java code..."
javac -cp ".:weka.jar" ConvertToARFF.java
javac -source 8 -target 8 -cp ".:weka.jar" ConvertToARFF.java
javac -source 8 -target 8 -cp ".:weka.jar" WekaClassification.java
javac -cp ".:weka.jar" WekaClassification.java
javac -cp ".:weka.jar" FeatureAnalysis.java

# Step 3: Convert Step4 output to ARFF format
echo "Converting output to ARFF format..."
java -cp ".:weka.jar" ConvertToARFF

# Step 4: Analyze features (but keep all features)
echo "Analyzing features..."
java -cp ".:weka.jar" FeatureAnalysis

# Step 5: Run classification and evaluation
echo "Running classification and evaluation..."
java -cp ".:weka.jar" WekaClassification

echo "===== Pipeline completed ====="
echo "Results are available in:"
echo "- classification_results.csv (classification metrics)"
echo "- feature_ranking.csv (feature importance ranking)"
echo "- semantic_similarity_processed.arff (processed dataset with all features)"