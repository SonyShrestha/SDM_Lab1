import os
import pandas as pd
import spacy
import re
from spacy.lang.en.stop_words import STOP_WORDS

field='property-graph'

def preprocessing(text):
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
    cleaned_keywords = [keyword for keyword in cleaned_keywords if ' ' in keyword]  
    
    return ', '.join(sorted(set(cleaned_keywords), key=cleaned_keywords.index))


def process_csv(input_csv, output_csv):
    nlp_model = spacy.load("en_core_web_sm")
    
    df = pd.read_csv(input_csv)
    df['abstract'] = df['abstract'].astype(str).apply(preprocessing)
    df['keywords'] = df['keywords'].astype(str)
    
    def combine_keywords(row):
        existing_keywords = list(filter(None, [kw.strip() for kw in row['keywords'].split(',')]))
        # Extract, split, and clean new keywords
        new_keywords = list(filter(None, [kw.strip() for kw in extract_keywords(row['abstract'], nlp_model).split(',')]))
        existing_set = set(existing_keywords)
        # Combine keywords, ensuring existing ones are first and no duplicates are introduced
        combined_keywords = existing_keywords + [kw for kw in new_keywords if kw not in existing_set]
        
        return ', '.join(combined_keywords)
    
    # Apply the combine_keywords function to each row
    df['keywords'] = df.apply(combine_keywords, axis=1)
    df.to_csv(output_csv, index=False)


if __name__ == "__main__":
    folder_path = 'paper_details'
    filename = 'paper_' + field + '.csv'

    input_csv = os.path.join(folder_path, filename)
    output_csv = os.path.join(folder_path, filename)
    
    process_csv(input_csv, output_csv)
