# Semantic Similarity Project

## Authors
- Tamir Nizri (211621552)
- Eyal Segev (315144717)
- Lior Hagay (314872367)

## Project Overview
This project implements a semantic similarity analysis system using Hadoop Map-Reduce. It processes Google's Syntactic N-Grams dataset to create co-occurrence vectors that represent semantic relationships between words. The system works by analyzing word pairs in sentences and calculating their contextual relationships across a large corpus of text.

## Input
The system uses the English All-Biarcs dataset from Google Syntactic N-Grams as input. This dataset contains syntactic dependencies between words in English sentences, providing rich contextual information for semantic analysis.

## Output
The final output consists of co-occurrence vectors that represent the semantic relationships between words. These vectors capture how words appear together in various contexts, allowing for semantic similarity comparisons.

## Running the Project

#### 1. Update your AWS credentials and Prepare the JAR Files (\*):

We recommend to use nano for loading your AWS credentials.
Command: nano ~/.aws/credentials

Use Maven to package the project into separate JARs for each Step.

Command: mvn clean package

Modify the pom.xml to specify the main class for the JAR you are creating.

### 2. Upload the JARs (*):

Ensure that all JAR ﬁles for the steps are uploaded to the correct S3 paths as needed.

### 3. Execute the Application:**

Run App.java locally to run the EMR jobs.
command:
```bash
mvn exec:java "-Dexec.mainClass=App"
```

### 4. Classification and Evaluation:**

Make sure to set the right path: hadoop/Semantic_Similarity/Hadoop/src/main/java and then

Convert the output from the EMR Job Flow to ARFF format:
Compile: javac -cp ".:weka.jar" ConvertToARFF.java
Run: java -cp ".:weka.jar" ConvertToARFF

Classification and Evaluation:
Compile: javac -cp ".:weka.jar" WekaClassification.java
Run: java -cp ".:weka.jar" WekaClassification

## Project Description

### Step 1: Count(F is f) Calculation
- **Mapper**: Emits (feature, headword) pairs with feature counts from n-grams. In this Step we assure that the headwords we are analyzing are only headwords in the golden-standard dataset.
- **Reducer**: Calculates total feature occurrences (count(F,f)) across corpus
- **Output Format**: headword → one feature with count_F_is_f, general count
- **Partitioner**: Partitions according to the feature (the key)

### Step 2: Reorganizing the list of features for the headwords
- **Mapper**: Associates features with sentences, using (headword + sentence) as key
- **Reducer**: Reorganizes features with their count_F_is_f values
- **Output Format**: headword → all features with count_F_is_f and feature counts
- **Partitioner**: Partitions according to the headword + sentence (the key)

### Step 3: Count(f,l), Count(F), Count(L) Calculation
- **Mapper**: Computes count_F (total features) and count_L (total words)
- **Reducer**: Calculates count_f_with_l (feature-word co-occurrences)
- **Output Format**: headword → features with count_f_with_l and count_F_is_f
- **Partitioner**: Partitions according to the headword (the key) 

### Step 4: Measuring and Distances calculation
- **Mapper**: Filters vectors for relevant headword pairs, calculates features using four methods, and emits ordered pairs with computed vectors.
- **Reducer**: Calculates distances for each headword pair using six methods, emitting a 24-value vector per pair.
- **Partitioner**: Partitions according to the pair of words (the key) 

## System Requirements

### Prerequisites
- Java 8 or higher
- Apache Hadoop 3.x
- Maven 3.6+
- Minimum 8GB RAM
- Storage space sufficient for dataset processing

### Environment Setup
1. Ensure Hadoop is properly configured
2. Configure environment variables
3. Verify Maven installation
