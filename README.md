# Semantic Similarity Project

## Authors
- Tamir Nizri (211621552)
- Eyal Segev (315144717)
- Lior Hagay (314872367)

## Project Overview
This project implements a semantic similarity analysis system using Hadoop MapReduce. It processes Google's Syntactic N-Grams dataset to create co-occurrence vectors that represent semantic relationships between words. The system works by analyzing word pairs in sentences and calculating their contextual relationships across a large corpus of text.

## Input
The system uses the English All-Biarcs dataset from Google Syntactic N-Grams as input. This dataset contains syntactic dependencies between words in English sentences, providing rich contextual information for semantic analysis.

## Output
The final output consists of co-occurrence vectors that represent the semantic relationships between words. These vectors capture how words appear together in various contexts, allowing for semantic similarity comparisons.

## Running the Project
To run the project, use the following Maven command:
```bash
mvn exec:java -Dexec.mainClass="App"
```

## Project Description

### Step 1: Feature Count Calculation
- **Mapper**: Emits (feature, headword) pairs with feature counts from n-grams
- **Reducer**: Calculates total feature occurrences (count(F,f)) across corpus
- **Output Format**: headword → feature with count_F_is_f, feature count
- **Partitioner**: Ensures even distribution of data across reducers

### Step 2: Sentence Context Processing
- **Mapper**: Associates features with sentences, using (headword + sentence) as key
- **Reducer**: Reorganizes features with their count_F_is_f values
- **Output Format**: headword → features with count_F_is_f and feature counts
- **Partitioner**: Distributes workload based on word occurrences

### Step 3: Corpus Statistics
- **Mapper**: Computes count_F (total features) and count_L (total words)
- **Reducer**: Calculates count_f_with_l (feature-word co-occurrences)
- **Output Format**: headword → features with count_f_with_l and count_F_is_f
- **Partitioner**: Balances vector computation load

### Step 4: Final Processing
- **Mapper**: Normalizes vectors
- **Reducer**: Produces final semantic similarity scores
- **Partitioner**: Ensures efficient final processing

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
