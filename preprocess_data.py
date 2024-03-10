import pandas as pd
import ast 
import nltk
import numpy as np
import re
import random
import os

input_folder='paper_details'
output_folder='preprocessed'

# Combine all csv files into one 
def combine_csv_files(folder_path, output_file):
    # Get list of CSV files in the folder
    csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]
    
    # Initialize an empty list to store DataFrame objects
    df_list = []
    
    # Read each CSV file and append its DataFrame to df_list
    for csv_file in csv_files:
        file_path = os.path.join(folder_path, csv_file)
        df = pd.read_csv(file_path)
        df_list.append(df)
    
    # Concatenate all DataFrames in df_list into a single DataFrame
    combined_df = pd.concat(df_list, ignore_index=True)
    
    return combined_df


def replace_blank_with_nan(value):
    if isinstance(value, str) and value.strip() == '':
        return np.nan
    return value


def extract_edition(text):
    match = re.search(r'\b(\d+)(?=th\b)', str(text))  # Ensure text is converted to string
    if match:
        number = match.group(1)
        return number 
    return   # Return string "0" instead of integer 0



def determine_type(row):
    venue_name = str(row['venue'])
    venue_type = str(row['publicationVenueType'])
    
    if venue_name and 'workshop' in venue_name.lower():
        return 'Workshop'
    elif venue_type and venue_type.lower() == 'conference':
        return 'Conference'
    elif venue_type and venue_type.lower() == 'journal':
        return 'Journal'
    else:
        return 'Unknown'


    
def preprocess_data():
    df = combine_csv_files(input_folder, output_folder+'/papers.csv')

    # Extract Publication Venue and Publication Type
    df['publicationVenue'] = df['publicationVenue'].apply(lambda x: ast.literal_eval(x) if pd.notna(x) else [])
    df['publicationVenueType'] = df['publicationVenue'].apply(lambda x:x.get('type')  if isinstance(x, dict) else None)
    df['jcwName'] = df['publicationVenue'].apply(lambda x:x.get('name')  if isinstance(x, dict) else None)
    
    # Extract author details
    df['authors'] = df['authors'].apply(ast.literal_eval)
    df['authors'] = df['authors'].apply(lambda x: x[:10] if isinstance(x, list) else [])
    df['authorId'] = df['authors'].apply(lambda x:  ','.join(str(author['authorId']) for author in x))
    df['authorName'] = df['authors'].apply(lambda x: ','.join(str(author['name']) for author in x))
    df['correspondingAuthorId'] = df['authorId'].str.split(',').str[0]

    # Extract journal details
    df['journal'] = df['journal'].apply(lambda x: ast.literal_eval(x) if pd.notna(x) else [])

    # Identify workshop, conference or journal
    df['type_indicator'] = df.apply(determine_type, axis=1)
    paper_ids_list= df.loc[df['type_indicator'] != 'Unknown', 'paperId'].tolist()
    
    # Set citationCount
    df['citationCount'] = df['citationCount'].apply(lambda x: random.randint(1, len(paper_ids_list)-1) if x > len(paper_ids_list)-1 else x)

    # Extract cited paper details
    df['citations'] = df['citations'].apply(ast.literal_eval)
    df['citedPaperId'] = df.apply(lambda row: ', '.join(map(str, random.sample([id_ for id_ in paper_ids_list if id_ != row['paperId']], row['citationCount']))), axis=1)
    df['journalVolume'] = df['journal'].apply(lambda x:x.get('volume')  if isinstance(x, dict) else None)


    df.drop(columns=['publicationVenue','authors','citations','journal','venue','publicationTypes'], inplace=True)
    df.to_csv(output_folder+'/papers.csv', index=False)
    

preprocess_data()