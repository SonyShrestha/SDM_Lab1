from neo4j import GraphDatabase
import pandas as pd
import argparse
import json
import logging
import time
import configparser
import get_paper_ids, get_paper_details, preprocess_data, split_files
import os
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()

config = configparser.ConfigParser()
config.read("config.ini")


class Neo4jConnector:
    def __init__(self, uri, user, password):
        self._uri = uri
        self._user = user
        self._password = password
        self._driver = None

    def connect(self):
        self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))

    def close(self):
        if self._driver is not None:
            self._driver.close()

    def run_query(self, query, parameters=None):
        with self._driver.session() as session:
            result = session.run(query, parameters)
            return result.data()  # return data only
    
    def execute_commands_from_file(self, file_path, timetrack):
        query_exec_time=[]
        number_of_rows=[]
        with open(file_path, 'r', encoding='utf-8') as file:
            commands = file.read().split(';')  # Split commands by semicolon
            with self._driver.session() as session:
                for command in commands:
                    if command.strip():  # Skip empty commands
                        if timetrack:
                            start_time = time.time()
                            result=session.run(command)
                            number_of_rows.append(len(result.data()))
                            end_time = time.time()
                            execution_time = round(end_time - start_time,4)
                            query_exec_time.append(execution_time)
                        else:
                            session.run(command)
        return query_exec_time,number_of_rows

def copy_files(source_dir, destination_dir):
    # Ensure the source directory exists
    if not os.path.exists(source_dir):
        print(f"Source directory '{source_dir}' does not exist.")
        return

    # Ensure the destination directory exists
    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)

    # Get list of files in source directory
    files = os.listdir(source_dir)

    # Copy each file to the destination directory
    for file in files:
        source_file = os.path.join(source_dir, file)
        destination_file = os.path.join(destination_dir, file)
        shutil.copy(source_file, destination_file)
        print(f"Copied '{source_file}' to '{destination_file}'")


def clear_directory(directory):
    # Check if the directory exists
    if not os.path.exists(directory):
        print(f"Directory '{directory}' does not exist.")
        return
    
    # Iterate over the contents of the directory
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path):
                # Remove file
                os.unlink(file_path)
                print(f"Removed file: {file_path}")
            elif os.path.isdir(file_path):
                # Remove directory and its contents recursively
                shutil.rmtree(file_path)
                print(f"Removed directory and its contents: {file_path}")
        except Exception as e:
            print(f"Error removing {file_path}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Implementation of graph database")
    
    # Extract config from config.ini file
    uri = config["NEO4J"]["uri"]
    user = config["NEO4J"]["user"]
    password = config["NEO4J"]["password"]
    api_key = config["API"]["api_key"]
    iterations = config["SETTINGS"]["iterations"]
    import_dir = config["NEO4J"]["import_dir"]

    # Add arguments
    parser.add_argument("--field", required=True, help="Comma separated fields to search for papers")
    # Parse arguments
    args = parser.parse_args()
    fields = args.field.split(',')

    logger.info("--------------------- CLEARING DIRECTORIES ---------------------")
    clear_directory('paper_ids')
    clear_directory('paper_idetails')
    clear_directory('preprocessed')
    clear_directory('splitted_files')

    logger.info("--------------------- FETCHING PAPER IDS ---------------------")
    # Retrieve paper information
    for field in fields:
        paper_info = get_paper_ids.get_paper_info(api_key, field, iterations)
        if paper_info:
            with open(f'paper_ids/paper_{field}.json', mode='w', encoding='utf-8') as file:
                json.dump(paper_info, file, indent=4)
        else:
            print("Failed to retrieve paper information.")


    logger.info("--------------------- FETCHING PAPER DETAILS FROM EXTRACTED PAPER IDS ---------------------")
    for field in fields:
        get_paper_details.fetch_publications(field,api_key)
    

    logger.info("--------------------- PREPROCESSING PAPER DETAILS ---------------------")
    preprocess_data.preprocess_data()


    logger.info("--------------------- SPLITTING PAPER DETAILS INTO SEPARATE FILES ---------------------")
    split_files.get_years()
    split_files.get_authors()
    split_files.get_keywords()
    split_files.get_journals()
    split_files.get_workshops()
    split_files.get_conferences()
    split_files.get_conference_papers()
    split_files.get_journal_papers()
    split_files.get_workshop_papers()
    split_files.get_organizations()

    logger.info("--------------------- Copy files to NEO4J import directory ---------------------")
    copy_files(os.getcwd() + "\\splitted_files", import_dir)


    logger.info("--------------------- Load data into NEO4J Graph ---------------------")
    connector = Neo4jConnector(uri, user, password)
    connector.connect()
    connector.execute_commands_from_file("scripts/load_data.cypher", False)

    connector.close()
