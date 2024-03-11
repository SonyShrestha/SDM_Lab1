import os
import pandas as pd
import spacy
from spacy.lang.en.stop_words import STOP_WORDS

def extract_keywords(text, nlp_model):
    text = str(text).lower()
    doc = nlp_model(text)
    
    # Extract noun phrases, excluding those with stopwords or short words
    keywords = [chunk.text for chunk in doc.noun_chunks if not any(token.is_stop or len(token.text) <= 2 for token in chunk)]
    # Clean keywords to keep only multi-word phrases 
    cleaned_keywords = [' '.join(part for part in keyword.split() if part not in STOP_WORDS) for keyword in keywords]
    cleaned_keywords = [keyword for keyword in cleaned_keywords if ' ' in keyword]  
    
    return ', '.join(sorted(set(cleaned_keywords), key=cleaned_keywords.index))

def process_csv(input_csv, output_csv):
    nlp_model = spacy.load("en_core_web_sm")
    
    # Extract keywords from abstract
    df = pd.read_csv(input_csv)
    df['abstract'] = df['abstract'].astype(str)
    df['keywords_new'] = df['abstract'].apply(lambda x: extract_keywords(x, nlp_model))
    df.to_csv(output_csv, index=False)

if __name__ == "__main__":
    folder_path = 'paper_details'  

    input_csv = os.path.join(folder_path, 'paper_property-graph.csv')
    output_csv = os.path.join(folder_path, 'paper_property_graph_with_keywords.csv')
    
    process_csv(input_csv, output_csv)
