from neo4j import GraphDatabase
import pandas as pd
import argparse
import json
import logging
import configparser
import time
import get_paper_ids, get_paper_details, preprocess_data, split_files


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
                            execution_time = end_time - start_time
                            query_exec_time.append(execution_time)
                            print(query_exec_time)
                        else:
                            session.run(command)


def top_3_cited_paper(connector):
    # Top 3 most cited papers of each conference.
    query="""
        MATCH (p:paper)-[:PUBLISHED_IN]->(c:conference)
        MATCH (p1:paper)-[:CITES]->(p)
        WITH c, p.paperId AS paperId, count(distinct p1.paperId) AS citationCount
        ORDER BY c.name ASC, citationCount DESC
        WITH c, collect({paperId: paperId, citationCount: citationCount}) AS papersByConference
        RETURN c.name AS ConferenceName, papersByConference[0..3] AS TopPapers
    """    
    result_data = connector.run_query(query)
    df = pd.DataFrame(result_data)
    print(df)


def conference_community(connector):
    # For each conference nd its community: i.e., those authors that have published papers on that conference in, at least, 4 different editions.
    query="""
        MATCH (p:paper)-[:WRITTEN_BY]->(a:author)
        MATCH (p)-[pi:PUBLISHED_IN]->(c:conference)
        WITH a.authorId AS authorId, c.name AS conferenceName, COUNT(DISTINCT pi.edition) AS numDistinctEditions
        WHERE numDistinctEditions >= 4
        RETURN conferenceName, COLLECT(authorId) AS community
    """    
    result_data = connector.run_query(query)
    df = pd.DataFrame(result_data)
    print(df)


def h_index(connector):
    # h-indexes of the authors in graph
    query="""
        MATCH (p:paper)-[:WRITTEN_BY]->(a:author) // MATCH (p:paper)-[:WRITTEN_BY]->(a:author{authorId:"144245703"})
        MATCH (citedByPaper:paper)-[:CITES]->(p)
        WITH a.authorId AS authorId, p.paperId AS paperId, count(distinct citedByPaper.paperId) AS citationCount
        ORDER BY authorId, paperId, citationCount DESC
        WITH authorId, collect(citationCount) AS citationCounts
        WITH authorId, citationCounts, [x IN range(0, size(citationCounts)-1) | x+1] AS idx
        WITH authorId, citationCounts, idx
        WITH authorId, citationCounts, idx, [i IN range(0, size(citationCounts)-1) | citationCounts[i] - idx[i]] AS differences
        WITH authorId, size([d IN differences WHERE d >= 0]) AS h_index
        RETURN authorId, h_index
    """    
    result_data = connector.run_query(query)
    df = pd.DataFrame(result_data)
    print(df)

def recommender(connector):
    # h-indexes of the authors in graph
    query="""
        // Define keywords for database community
        WITH ['Data Management', 'Indexing', 'Data Modeling', 'Big Data', 'Data Processing', 'Data Storage', 'Data Querying', ' Property Graph'] AS databaseCommunityKeywords
        UNWIND databaseCommunityKeywords AS keyword

        // if 90% of the papers published  in a conference/journal contain one of the keywords of the database community we consider that conference/journal as related to that community
        MATCH (p:paper)-[:PUBLISHED_IN]->(jc:journal|conference)
        OPTIONAL MATCH (p)-[:HAS_KEYWORD]->(k:keyword{keyword: keyword})
        WITH
            jc.name AS journalOrConference, 
            COUNT(DISTINCT p.paperId) AS numPaperPublished, SUM(CASE WHEN k.keyword IS NOT NULL THEN 1 ELSE 0 END) AS numPaperPublishedComm
        WHERE numPaperPublishedComm > 0 AND (numPaperPublishedComm / numPaperPublished) * 100 > 90

        //  Identify the top papers of these conferences/journals
        WITH journalOrConference
        MATCH (p1:paper)-[:PUBLISHED_IN]->(jc1:journal|conference {name: journalOrConference})
        WITH jc1, p1.paperId AS paper_Id, toInteger(p1.citationCount) AS citationCount
        ORDER BY jc1.name ASC, citationCount DESC
        WITH jc1.name AS conferenceName, collect({paperId: paper_Id, citationCount: citationCount}) AS papersByConference
        WITH conferenceName AS ConferenceName, REDUCE(acc = [], paperData IN papersByConference | acc + paperData)[0..100] AS TopPapers
        UNWIND TopPapers AS paper

        // Identify good match to review database papers and gurus
        WITH paper.paperId AS PaperId
        MATCH (p2:paper{paperId: PaperId})-[:WRITTEN_BY]->(a1:author)
        RETURN a1.name AS potentialGoodMatch, CASE WHEN count(distinct p2.paperId) >= 2 THEN 'Yes' ELSE 'No' END AS isGuru
    """    
    result_data = connector.run_query(query)
    df = pd.DataFrame(result_data)
    print(df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Implementation of graph database")
    
    # Extract config from config.ini file
    uri = config["NEO4J"]["uri"]
    user = config["NEO4J"]["user"]
    password = config["NEO4J"]["password"]

    logger.info("--------------------- EVOLVING GRAPH ---------------------")
    connector = Neo4jConnector(uri, user, password)
    connector.connect()

    logger.info("--------------------- Load data into NEO4J Graph ---------------------")
    connector.execute_commands_from_file("scripts/queries.cypher", False)


    # top_3_cited_paper(connector)
    # conference_community(connector)
    # h_index(connector)

    # recommender(connector)

    # Convert the result to a DataFrame
    connector.close()
