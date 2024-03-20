import pandas as pd
import ast 
import nltk
import numpy as np
import re
import random
import os
import hashlib
import logging
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()

# Define input and output directories for extracting preprocessing paper information
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


# Extract edition from conference name
def extract_edition(year):
    match = re.search(r'\b(\d+)(?=th\b)', str(year)) 
    if match:
        number = match.group(1)
        return number 
    return int(str(year)[-2:])  


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


def extract_citation_id(df_row,date_paper_dict):
    filtered_paper_ids = [paper_id for paper_ids in {publicationDate: paper_ids for publicationDate, paper_ids in date_paper_dict.items() if publicationDate < df_row["publicationDate"]}.values() for paper_id in paper_ids]
    if df_row["paperId"] in filtered_paper_ids:
            filtered_paper_ids.remove(df_row["paperId"])
    return random.sample(filtered_paper_ids,min(len(filtered_paper_ids), df_row['citationCount']))
    
def duplicate_rows(row):
    dup_paper = row.copy()  # Make a copy of the original row
    # Modify one column in the duplicated row, for example, double the value in column 'A'
    dup_paper['paperId'] = str(uuid.uuid4())
    try:
        dup_paper['title'] = ast.literal_eval(row['references'])[0]['title']
    except Exception as e:
        dup_paper['title'] = "Big Data Technology "+ row['title'].strip("\"")
        print(dup_paper['title'])
    dup_paper['edition'] = int(row['edition'])-1 if int(row['edition'])>=1 else 21

    return dup_paper


def preprocess_data():
    logger.info('Combining CSV files generated using all field type')
    df = combine_csv_files(input_folder, output_folder+'/papers.csv')
    

    # Convert 'date_column' to datetime format with a custom format
    df['publicationDate'] = pd.to_datetime(df['publicationDate'], format='%Y-%m-%d')

    # Convert 'year' to string, concatenate with '01-01', and convert to datetime
    default_year=2000
    df['year'] = df['year'].fillna(default_year).astype(int)
    default_date = pd.to_datetime(df['year'].astype(str) + '-01-01')  
    df['publicationDate'] = df['publicationDate'].fillna(default_date)

    # Extract Publication Venue and Publication Type
    df['publicationVenue'] = df['publicationVenue'].apply(lambda x: ast.literal_eval(x) if pd.notna(x) else [])
    df['publicationVenueType'] = df['publicationVenue'].apply(lambda x:x.get('type')  if isinstance(x, dict) else None)
    df['jcwName'] = df['publicationVenue'].apply(lambda x:x.get('name')  if isinstance(x, dict) else '')
    df['jcwName'] = df.apply(lambda x:x['venue']  if x['jcwName']=='' else x['jcwName'], axis=1)

    # Extract author details
    df['authors'] = df['authors'].apply(ast.literal_eval)
    df['authors'] = df['authors'].apply(lambda x: x[:10] if isinstance(x, list) else [])
    df['authorId'] = df['authors'].apply(lambda x:  ','.join(str(author['authorId'] if author['authorId'] != None else 987654321+df.index[0]) for author in x))
    
    df['authorId'] = df['authorId'].fillna('0')
    df = df[(df['authorId'] != '') & (df['authorId'] != 'None')]
    df['authorName'] = df['authors'].apply(lambda x: ','.join(str(author['name']) for author in x))

    logger.info('Generating synthetic data for corresponding author')
    df['correspondingAuthorId'] = df['authorId'].str.split(',').str[0].astype(str).apply(lambda x: re.sub(r'\.0$', '', x))


    # Extract journal details
    df['journal'] = df['journal'].apply(lambda x: ast.literal_eval(x) if pd.notna(x) else [])

    # Identify workshop, conference or journal
    logger.info('Identifying type of publication as Conference/Journal/Workshop')
    df['type_indicator'] = df.apply(determine_type, axis=1)
    paper_ids_list= df.loc[df['type_indicator'] != 'Unknown', 'paperId'].tolist()
    
    # Set citationCount
    df['citationCount'] = df['citationCount'].apply(lambda x: random.randint(1, len(paper_ids_list)-1) if x > len(paper_ids_list)-1 else x)

    # Extract cited paper details
    logger.info('Generating synthetic data for citations')
    df['citations'] = df['citations'].apply(ast.literal_eval)
    date_paper_dict = df.groupby('publicationDate')['paperId'].apply(list).to_dict()
    df['citedPaperId'] = df.apply(lambda row: ','.join(str(i) for i in extract_citation_id(row, date_paper_dict)), axis=1)
    df['journalVolume'] = df['journal'].apply(lambda x:x.get('volume')  if isinstance(x, dict) else None)
    df['citationCount'] = df['citedPaperId'].apply(lambda x: len(x.split(',')))


    df.drop(columns=['publicationVenue','authors','citations','journal','venue','publicationTypes'], inplace=True)
    df = df[df['type_indicator'] != 'Unknown']

    df['edition'] = df['year'].apply(extract_edition)

    # Duplicate each row in the DataFrame and apply the function
    duplicated_rows = df.apply(duplicate_rows, axis=1)

    # Concatenate the original DataFrame and the duplicated rows
    result_df = pd.concat([df, duplicated_rows], ignore_index=True)

    

    result_df.to_csv(output_folder+'/papers.csv', index=False)
    
def main():
    preprocess_data()

if __name__ == "__main__":
    main()