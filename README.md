# Semantic Data Management Lab 1

## Description 
This repository contains the implementation of a research publication graph model as per requirement provided [here](https://github.com/SonyShrestha/SDM_Lab1/blob/main/property-graphs.pdf). Initially, the data was extracted using the Semantic Scholar API and stored in JSON format. The JSON file was then preprocessed and split into multiple files for efficient loading into Neo4j using Cypher queries. Things implemented in this project are mentioned below.
1. Extraction of paper details using Semantic Scholar API.
2. Preprocessing and splitting of JSON files.
3. Cypher queries for various tasks related to research publications.
4. Evolution section: Modifications made to adapt the graph model to changes.
5. Recommender section: Cypher query to identify potential reviewers for the database community.
6. Implementation of graph algorithms:
    - Finding Similarity between two papers
    - Finding PageRank of publications

<br>

## Installation 
All required libraries can be installed using requirements.txt file.
```
pip install -r requirements.txt
```
<br>

## Code Execution 
```
python PartA2_ShresthaRaza.py --field field1,field2 --new_data yes/no
```

```
python PartA2_ShresthaRaza.py --field big-data,graph-processing --new_data yes
```

Argument **field** contains **comma separated fields to search for papers** <br>
Argument **new_data** specifies **whether you want to generate new data from Schemantic Scholar or not**

```
python PartA3_ShresthaRaza.py
```

```
python PartB_ShresthaRaza.py
```

```
python PartC_ShresthaRaza.py
```

```
python PartD_ShresthaRaza.py
```