from neo4j import GraphDatabase
import pandas as pd

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


def top_3_cited_paper(connector):
    # Top 3 most cited papers of each conference.
    query="""
        MATCH (p:paper)-[:PUBLISHED_IN]->(c:conference)
        OPTIONAL MATCH (p1:paper)-[:CITES]->(p)
        WITH c, p, count(p1) AS citationCount
        ORDER BY citationCount DESC
        WITH c, COLLECT({paperId: p.paperId, citationCount: citationCount}) AS papers
        RETURN c.name AS Conference, 
            [paper IN papers | paper][..3] AS Top3Papers
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
        MATCH (p:paper)-[:WRITTEN_BY]->(a:author)
        WITH a.authorId AS AuthorId, toInteger(p.citationCount) AS CitationCount
        ORDER BY AuthorId, CitationCount DESC
        WITH AuthorId, collect(CitationCount) AS CitationCounts
        WITH AuthorId, CitationCounts, reduce(s = 0, x IN CitationCounts | CASE WHEN x = CitationCounts[0] THEN 1 ELSE s + 1 END) AS Rank
        WITH AuthorId, CitationCounts, Rank, CitationCounts[-1] - toInteger(Rank) AS Difference
        WHERE CitationCounts[-1] > Difference
        RETURN AuthorId, count(*) as h_index
    """    
    result_data = connector.run_query(query)
    df = pd.DataFrame(result_data)
    print(df)


if __name__ == "__main__":
    # Example usage
    uri = "bolt://localhost:7687"  # Replace with your Neo4j server URI
    user = "neo4j"         # Replace with your Neo4j username
    password = "abcd1234"     # Replace with your Neo4j password

    connector = Neo4jConnector(uri, user, password)
    connector.connect()
    
    top_3_cited_paper(connector)
    conference_community(connector)
    h_index(connector)

    # Convert the result to a DataFrame
    connector.close()
