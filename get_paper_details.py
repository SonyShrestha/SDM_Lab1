import json
import pandas as pd
import requests
import random
import csv
import os

input_folder = 'paper_ids'
output_folder='paper_details'

# Function to assign keywords based on from which field was the paper details generated
def assign_keywords(field):
    keywords={
        "data-quality": ["Data accuracy", "Data completeness", "Data consistency", "Data integrity", "Data validation", "Data verification", "Data cleaning", "Data standardization", "Data governance", "Data profiling", "Data auditing", "Data enrichment", "Error detection", "Data reliability", "Data timeliness", "Quality metrics", "Data compliance", "Data lineage", "Data quality management", "Data improvement techniques"],
        "data-storage": ["Data warehousing", "Database management systems (DBMS)", "Cloud storage", "Object storage", "File storage", "Block storage", "Data lakes", "Distributed file systems", "Network Attached Storage (NAS)", "Storage Area Network (SAN)", "Solid State Drives (SSD)", "Hard Disk Drives (HDD)", "Data replication", "Data archiving", "Data backup", "Data retention policies", "Data encryption", "Storage efficiency", "Data compression", "Hierarchical storage management"],
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
def publications(field):
    r = requests.post(
        'https://api.semanticscholar.org/graph/v1/paper/batch',
        params={'fields': 'paperId,authors,title,venue,publicationVenue,year,abstract,citationCount,fieldsOfStudy,s2FieldsOfStudy,publicationTypes,publicationDate,citations,journal,references'},
        json={"ids": get_paper_ids(field)}
    )
    json_data = r.json()
    for idx, data in enumerate(json_data):
        for key, value in data.items():
            if isinstance(value, str):
                json_data[idx][key] = value.replace('\r', ' ').replace('\n', ' ')
    
    return json_data


# Dump fetched publication details into CSV file
def fetch_publications(field):
    # Fetch publications data
    publications_data = publications(field)
    df = pd.DataFrame(publications_data)

    filtered_df = df[df['paperId'].apply(lambda x: isinstance(x, str) and len(x) > 15 and x.isalnum())]
    filtered_df["keywords"]=filtered_df["paperId"].apply(lambda x:assign_keywords(field))
    filtered_df.to_csv(output_folder+'/paper_' + field + '.csv', index=False, quoting=csv.QUOTE_ALL)


def main():
    field_names = [file_name.split('_')[1].split('.')[0] for file_name in os.listdir('paper_ids')]
    for field in field_names:
        fetch_publications(field)

if __name__ == "__main__":
    main()