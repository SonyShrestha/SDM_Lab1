import json
import pandas as pd
import requests
import random
import csv
import argparse
import logging
import re
import spacy
from spacy.lang.en.stop_words import STOP_WORDS

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()

# Define input and output directories for extracting paper details from paper ids
input_folder = 'paper_ids'
output_folder='paper_details'


# Function to assign keywords based on from which field was the paper details generated
def assign_keywords(field):
    keywords={
        "property-graph":["Graph database", "Graph analytics", "Graph modeling", "Graph algorithms", "Graph neural networks", "Graph embeddings", "Graph querying", "Graph visualization", "Property graph schema", "Graph mining", "Graph partitioning", "Graph indexing", "Graph representation learning", "Property graph query languages", "Graph similarity", "Graph pattern matching", "Graph storage", "Graph clustering", "Graph-based recommendation"],
        "big-data":["Data analytics", "Data processing", "Data mining", "Data visualization", "Machine learning", "Artificial intelligence", "Deep learning", "Data storage", "Distributed systems", "Parallel computing", "Stream processing", "Batch processing", "Data management", "Data integration", "Data warehousing", "Hadoop"],
        "data-management":["Database systems", "Data governance", "Data quality", "Data integration", "Data architecture", "Data modeling", "Data warehousing", "Data migration", "Data lifecycle", "Master data management", "Metadata management", "Data catalog", "Data stewardship", "Data lineage", "Data security", "Data privacy", "Data governance", "Data cleaning", "Data retention", "Data access control"],
        "data-modeling":["Entity-relationship model", "Relational model", "Dimensional modeling", "Conceptual data model", "Logical data model", "Physical data model", "Data modeling tools", "ERD (Entity-Relationship Diagram)", "Normalization", "Denormalization", "Data dictionary", "Data abstraction", "Data schema", "Data modeling techniques", "Data modeling standards", "Schema design", "Database design", "Data architecture", "Data modeling best practices"],
        "data-processing":["ETL (Extract, Transform, Load)", "Data transformation", "Data aggregation", "Data enrichment", "Data normalization", "Data cleansing", "Data wrangling", "Data validation", "Data fusion", "Data deduplication", "Data imputation", "Data filtering", "Data summarization", "Data compression", "Data encryption", "Batch processing", "Stream processing", "Real-time processing"],
        "data-querying":["SQL (Structured Query Language)", "NoSQL", "Query optimization", "Query execution", "Query language", "Relational databases", "Database queries", "Query performance tuning", "Query planner", "Query analyzer", "Indexing", "Full-text search", "Search queries", "OLAP (Online Analytical Processing)", "OLTP (Online Transaction Processing)", "Data retrieval", "Ad-hoc queries", "Complex queries", "Query caching"],
        "data-science":["Machine learning", "Artificial intelligence", "Deep learning", "Data analysis", "Data mining", "Predictive analytics", "Statistical modeling", "Big data analytics", "Data visualization", "Natural language processing", "Feature engineering", "Exploratory data analysis", "Predictive modeling", "Classification", "Regression analysis", "Clustering", "Dimensionality reduction", "Model evaluation", "Model deployment"],
        "deep-learning":["Neural networks", "Artificial neural networks", "Convolutional neural networks (CNN)", "Recurrent neural networks (RNN)", "Long short-term memory (LSTM)", "Deep belief networks (DBN)", "Autoencoders", "Generative adversarial networks (GAN)", "Deep reinforcement learning", "Transfer learning", "Natural language processing (NLP)", "Computer vision", "Image recognition", "Speech recognition", "Time series analysis", "Sequence-to-sequence learning", "Attention mechanisms", "Deep learning frameworks", "TensorFlow", "PyTorch", "Keras"],
        "graph-processing":["Graph algorithms", "Graph traversal", "Graph analytics", "Graph partitioning", "Graph databases", "Graph querying", "Graph visualization", "Distributed graph processing", "Parallel graph processing", "Large-scale graph processing", "Graph-based machine learning", "Community detection", "Centrality measures", "PageRank algorithm", "Shortest path algorithms", "Connected components", "Graph embedding", "Graph neural networks", "Graph convolutional networks (GCN)", "Property graph"],
        "indexing":["Database indexing", "Index structure", "Primary index", "Secondary index", "Clustered index", "Non-clustered index", "Indexing methods", "B-tree", "Hash index", "Bitmap index", "Inverted index", "Spatial index", "Text indexing", "Full-text index", "Index optimization", "Index maintenance", "Index utilization", "Index seek", "Index scan", "Covering index", "Composite index"],
        "machine-learning":["Supervised learning", "Unsupervised learning", "Semi-supervised learning", "Reinforcement learning", "Deep learning", "Neural networks", "Artificial neural networks", "Convolutional neural networks (CNN)", "Recurrent neural networks (RNN)", "Long short-term memory (LSTM)", "Decision trees", "Random forests", "Support vector machines (SVM)", "k-Nearest neighbors (k-NN)", "Clustering", "K-means clustering", "Hierarchical clustering", "Dimensionality reduction", "Principal component analysis (PCA)", "Feature selection", "Model evaluation", "Cross-validation", "Hyperparameter tuning"]
    }
    # Lower each word
    words = [word.lower() for word in field.split('-')]

    # Join the words back together
    fixed_keyword = ' '.join(words)

    random_keywords = random.sample(keywords[field], 3)
    random_keywords_lower = [word.lower() for word in random_keywords]
    random_keywords_text = fixed_keyword + ', ' + ', '.join(random_keywords_lower)
    return random_keywords_text


# Get paper ids from JSON file
def get_paper_ids(field):
    # Read JSON data from file
    with open(input_folder+'/paper_' + field + '.json', 'r') as file:
        data = json.load(file)

    # Extract paper IDs
    paper_ids = [item['paperId'] for item in data['data']]

    return paper_ids


# Fetch paper details from API
def publications(field,api_key):
    headers = {"x-api-key": api_key}
    logger.info('Making API Call to fetch paper details from paper ids for field %s', field)
    response = requests.post(
        'https://api.semanticscholar.org/graph/v1/paper/batch',
        params={'fields': 'paperId,authors,title,venue,publicationVenue,year,abstract,citationCount,fieldsOfStudy,s2FieldsOfStudy,publicationTypes,publicationDate,citations,journal,references'},
        json={"ids": get_paper_ids(field)},
        headers=headers
    )
    if response.status_code == 200:
        json_data = response.json()
        for idx, data in enumerate(json_data):
            for key, value in data.items():
                if isinstance(value, str):
                    json_data[idx][key] = value.replace('\r', ' ').replace('\n', ' ')
        
        return json_data
    else:
        logger.error("Error: %s", response.status_code)  
        return None


def abstract_preprocessing(text):
    # Check if text is NaN or None and replace with empty string
    if pd.isna(text) or text.strip() == "":
        return ""
    # Convert to lowercase for consistent matching
    text = text.lower()    
    # Check for non-abstract content
    if "paperid" in text or "title" in text:
        return ""  
    # Remove non-alphabetic characters, keeping spaces
    text = re.sub(r"[^a-z\s]", '', text)    
    return text


def extract_keywords(text, nlp_model):
    if not text:  # If the preprocessed string is empty, return an empty string
        return ""
    
    doc = nlp_model(text)
    keywords = [chunk.text for chunk in doc.noun_chunks if not any(token.is_stop or len(token.text) <= 2 for token in chunk)]
    cleaned_keywords = [' '.join(part for part in keyword.split() if part not in STOP_WORDS) for keyword in keywords]
    cleaned_keywords = [keyword for keyword in cleaned_keywords if ' ' in keyword][:5]  
    
    return ', '.join(sorted(set(cleaned_keywords), key=cleaned_keywords.index))


def combine_keywords(row):
    nlp_model = spacy.load("en_core_web_sm")
    existing_keywords = list(filter(None, [kw.strip().title() for kw in row['keywords'].split(',')]))
    # Extract, split, and clean new keywords
    new_keywords = list(filter(None, [kw.strip().title() for kw in extract_keywords(row['abstract'], nlp_model).split(',')]))
    existing_set = set(existing_keywords)
    # Combine keywords, ensuring existing ones are first and no duplicates are introduced
    combined_keywords = existing_keywords + list([kw for kw in new_keywords if kw not in existing_set])[:4]
        
    return ', '.join(combined_keywords)


# Dump fetched publication details into CSV file
def fetch_publications(field,api_key):
    # Fetch publications data
    publications_data = publications(field, api_key)

    # Convert JSON data to DataFrame
    df = pd.DataFrame(publications_data)

    logger.info('Preprocessing field abstract')
    df['abstract'] = df['abstract'].astype(str).apply(abstract_preprocessing)

    logger.info('Generating synthetic data for keywords')
    df["keywords"]=df["paperId"].apply(lambda x:assign_keywords(field))
    df['keywords'] = df.apply(combine_keywords, axis=1)

    # Write DataFrame to CSV file
    df.to_csv(output_folder+'/paper_' + field + '.csv', index=False, quoting=csv.QUOTE_ALL)


def main():
    # Create argument parser
    parser = argparse.ArgumentParser(description="Retrieve paper details from Semantic Scholar API")

    # Add arguments
    parser.add_argument("--field", required=True, help="Comma separated fields to search for papers")
    parser.add_argument("--api_key", required=True, help="API key for accessing Semantic Scholar API")
    # Parse arguments
    args = parser.parse_args()
    fields = args.field.split(',')

    # Retrieve paper information
    for field in fields:
        fetch_publications(field,args.api_key)

if __name__ == "__main__":
    main()
