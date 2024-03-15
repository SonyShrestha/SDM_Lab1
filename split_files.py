import pandas as pd
import ast 
import nltk
import numpy as np
import re
import os
import random
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()

# Define input and output directories for splitting files
input_folder='preprocessed'
output_folder='splitted_files'


# Extract edition from conference name
def extract_edition(year):
    match = re.search(r'\b(\d+)(?=th\b)', str(year)) 
    if match:
        number = match.group(1)
        return number 
    return str(year)[-2:] 


# Assign reviwers
def assign_reviewers(authors):
    df = pd.read_csv(output_folder+'/authors.csv')
    potential_reviewers = df['authorId'].tolist()

    random_values = [value for value in potential_reviewers if value not in authors.split(',')]

    # Sample random values from the resulting list
    reviewers = random.sample(random_values, min(3, len(random_values)))

    return ','.join(map(str, reviewers))


# Fetch year details from CSV file
def get_years():
    # Read CSV file into DataFrame
    df = pd.read_csv(input_folder+'/papers.csv')

    # Convert 'year' column to numeric type and fill missing values with 0
    df['year'] = pd.to_numeric(df['year'])

    # Calculate min and max years
    min_year = int(df['year'].min())
    max_year = int(df['year'].max())

    # Create a DataFrame with the year range
    year_range = range(min_year, max_year + 1)
    year_df = pd.DataFrame({'year': year_range})

    # Write the DataFrame to a CSV file
    year_df.to_csv(output_folder+'/year.csv', index=False)


# Fetch journal details from CSV file
def get_journals():
    df_papers = pd.read_csv(input_folder+'/papers.csv')

    df = df_papers[df_papers['type_indicator'] == "Journal"]
    column_to_keep = ['jcwName']
    
    # Drop all columns except the one to keep
    columns_to_drop = [col for col in df.columns if col not in column_to_keep]
    df.drop(columns=columns_to_drop, inplace=True)
    
    # Drop Journals with empty name
    df = df.dropna() 
    
    # Drop duplicate journal names and keep only distinct values
    df = df.drop_duplicates()
    
    # Write distinct journal names to CSV file
    df.to_csv(output_folder+'/journals.csv', index=False)


# Fetch conference details from CSV file
def get_conferences():
    df_papers = pd.read_csv(input_folder+'/papers.csv')

    df = df_papers[df_papers['type_indicator'] == "Conference"]
    column_to_keep = ['jcwName']
    
    # Drop all columns except the one to keep
    columns_to_drop = [col for col in df.columns if col not in column_to_keep]
    df.drop(columns=columns_to_drop, inplace=True)
    
    # Drop Journals with empty name
    df = df.dropna() 
    
    # Drop duplicate journal names and keep only distinct values
    df = df.drop_duplicates()
    
    # Write distinct journal names to CSV file
    df.to_csv(output_folder+'/conferences.csv', index=False)


# Fetch workshop details from CSV file
def get_workshops():
    df_papers = pd.read_csv(input_folder+'/papers.csv')

    df = df_papers[df_papers['type_indicator'] == "Workshop"]
    column_to_keep = ['jcwName']
    
    # Drop all columns except the one to keep
    columns_to_drop = [col for col in df.columns if col not in column_to_keep]
    df.drop(columns=columns_to_drop, inplace=True)
    
    # Drop Journals with empty name
    df = df.dropna() 
    
    # Drop duplicate journal names and keep only distinct values
    df = df.drop_duplicates()
    
    # Write distinct journal names to CSV file
    df.to_csv(output_folder+'/workshops.csv', index=False)


# Fetch keywords from CSV file
def get_keywords():
    df = pd.read_csv(input_folder+'/papers.csv')

    column_to_keep = ['keywords']
    
    # Drop all columns except the one to keep
    columns_to_drop = [col for col in df.columns if col not in column_to_keep]
    df.drop(columns=columns_to_drop, inplace=True)
    
    df['keywords'] = df['keywords'].str.split(',')
    df = df.explode('keywords')
    df['keywords'] = df['keywords'].str.strip()

    # Drop Journals with empty name
    df = df.dropna() 
    
    # Drop duplicate journal names and keep only distinct values
    df = df.drop_duplicates()
    
    # Write distinct journal names to CSV file
    df.to_csv(output_folder+'/keywords.csv', index=False)


def get_affiliated_org(data):
    random_number = random.randint(1, 17)
    uni_or_comp = "University " if random.randint(0, 1)==0 else "Company "
    affiliatedOrg = uni_or_comp+ str(random_number)
    return affiliatedOrg


# Fetch authors details from CSV file
def get_authors():
    # Read CSV file containing authors
    df = pd.read_csv(input_folder + '/papers.csv')

    column_to_keep = ['authorId','authorName']
    
    # Drop all columns except the ones to keep
    columns_to_drop = [col for col in df.columns if col not in column_to_keep]
    df.drop(columns=columns_to_drop, inplace=True)

    # Split the values in the 'authorId' and 'authorName' columns
    df['authorId'] = df['authorId'].str.split(',')
    df['authorName'] = df['authorName'].str.split(',')

    new_dfs = []

    # Iterate over each row of the DataFrame
    for index, row in df.iterrows():
        # Transpose the lists using zip and construct a DataFrame
        new_df = pd.DataFrame(zip(*row), columns=df.columns)
        new_dfs.append(new_df)

    # Concatenate the list of DataFrames into a single DataFrame
    result_df = pd.concat(new_dfs, ignore_index=True)
    result_df.drop_duplicates(subset=['authorId', 'authorName'], inplace=True)

    # Assign affiliated organization
    result_df['affiliatedOrg'] = result_df.apply(get_affiliated_org, axis=1)
    
    # Write the DataFrame to a new CSV file
    result_df.to_csv(output_folder + '/authors.csv', index=False)


# Fetch journal papers from CSV file
def get_journal_papers():
    df = pd.read_csv(input_folder + '/papers.csv')

    # Extract only records where publicationVenueType="journal
    journal_df = df[df['type_indicator'] == "Journal"]

    column_to_keep = ['paperId','title','abstract','year','citationCount','publicationDate','keywords','jcwName','authorId','authorName','correspondingAuthorId','citedPaperId','journalVolume','reviewers']
    
    # Drop all columns except the ones to keep
    columns_to_drop = [col for col in journal_df.columns if col not in column_to_keep]
    journal_df.drop(columns=columns_to_drop, inplace=True)

    # Convert 'authorId' column to list of lists
    #journal_df["authorId"] = journal_df["authorId"].str.split(',')

    # Apply assign_reviewers function to each element of 'authorId' column
    journal_df["reviewers"] = journal_df["authorId"].apply(assign_reviewers)

    journal_df.to_csv(output_folder + '/journal_paper.csv', index=False, columns=column_to_keep)


# Fetch Conference papers from CSV file
def get_conference_papers():
    df = pd.read_csv(input_folder + '/papers.csv')

    # Extract only records where publicationVenueType="conference"
    conference_df = df[df['type_indicator'] == "Conference"]

    column_to_keep = ['paperId','title','abstract','year','citationCount','publicationDate','keywords','jcwName','authorId','authorName','correspondingAuthorId','citedPaperId','edition','reviewers']
    
    # Drop all columns except the ones to keep
    columns_to_drop = [col for col in conference_df.columns if col not in column_to_keep]
    conference_df.drop(columns=columns_to_drop, inplace=True)

    
    # Apply extract_edition function to 'journalName' column
    conference_df["edition"] = conference_df['year'].apply(extract_edition)

    # Apply assign_reviewers function to each element of 'authorId' column
    conference_df["reviewers"] = conference_df["authorId"].apply(lambda row: assign_reviewers(row))

    # Save the updated DataFrame to CSV
    conference_df.to_csv(output_folder + '/conference_paper.csv', index=False, columns=column_to_keep)


# Fetch Workshop papers from CSV file
def get_workshop_papers():
    df = pd.read_csv(input_folder+'/papers.csv')

    # Extract only records where publicationVenueType="journal"
    workshop_df = df[df['type_indicator'] == "Workshop"]

    column_to_keep = ['paperId','title','abstract','year','citationCount','publicationDate','keywords','jcwName','authorId','authorName','correspondingAuthorId','citedPaperId','edition','reviewers']
    
    # Drop all columns except the ones to keep
    columns_to_drop = [col for col in workshop_df.columns if col not in column_to_keep]
    workshop_df.drop(columns=columns_to_drop, inplace=True)

    # Apply extract_edition function to 'journalName' column
    workshop_df["edition"] = workshop_df['year'].apply(extract_edition)
    
    # Apply assign_reviewers function to each element of 'authorId' column
    workshop_df["reviewers"] = workshop_df["authorId"].apply(lambda row: assign_reviewers(row))

    workshop_df.to_csv(output_folder+'/workshop_paper.csv', index=False, columns=column_to_keep)


def main():
    logger.info('Generating CSV file for year')
    get_years()

    logger.info('Generating CSV file for authors')
    get_authors()

    logger.info('Generating CSV file for keywords')
    get_keywords()

    logger.info('Generating CSV file for journals')
    get_journals()

    logger.info('Generating CSV file for workshops')
    get_workshops()

    logger.info('Generating CSV file for conferences')
    get_conferences()

    logger.info('Generating CSV file for conference papers')
    get_conference_papers()

    logger.info('Generating CSV file for journal papers')
    get_journal_papers()

    logger.info('Generating CSV file for workshop papers')
    get_workshop_papers()


if __name__ == "__main__":
    main()