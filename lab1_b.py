from neo4j import GraphDatabase
import pandas as pd
import argparse
import json
import logging
import configparser
import get_paper_ids, get_paper_details, preprocess_data, split_files
import time


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
        with open(file_path, 'r', encoding='utf-8') as file:
            commands = file.read().split(';')  # Split commands by semicolon
            with self._driver.session() as session:
                for command in commands:
                    if command.strip():  # Skip empty commands
                        if timetrack:
                            start_time = time.time()
                            session.run(command)
                            end_time = time.time()
                            execution_time = round(end_time - start_time,4)
                            query_exec_time.append(execution_time)
                        else:
                            session.run(command)
        return query_exec_time


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Implementation of graph database")
    
    # Extract config from config.ini file
    uri = config["NEO4J"]["uri"]
    user = config["NEO4J"]["user"]
    password = config["NEO4J"]["password"]

    connector = Neo4jConnector(uri, user, password)
    connector.connect()

    logger.info("--------------------- EXECUTING QUERIES ---------------------")
    query_exec_time = connector.execute_commands_from_file("scripts/queries.cypher",True)
    df = pd.DataFrame(query_exec_time, columns=['EXECUTION TIME (in seconds)'])
    df['QUERY'] = df.index+1
    df = df[['QUERY', 'EXECUTION TIME (in seconds)']]
    print(df)

    connector.close()
