import pandas as pd
import argparse
import logging
import configparser
from neo4j_connector import Neo4jConnector



# Configure logging
logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()

config = configparser.ConfigParser()
config.read("config.ini")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Implementation of graph database")
    
    # Extract config from config.ini file
    uri = config["NEO4J"]["uri"]
    user = config["NEO4J"]["user"]
    password = config["NEO4J"]["password"]

    connector = Neo4jConnector(uri, user, password)
    connector.connect()

    logger.info("--------------------- EXECUTING QUERIES ---------------------")
    query_exec_time,number_of_rows = connector.execute_commands_from_file("scripts/queries.cypher",True)
    df = pd.DataFrame({'EXECUTION TIME (in seconds)': query_exec_time, 'NUMBER_OF_ROWS': number_of_rows})
    df['QUERY'] = df.index+1
    df = df[['QUERY', 'EXECUTION TIME (in seconds)','NUMBER_OF_ROWS']]
    print(df)

    connector.close()
