// Create node Community of type Database 
CREATE (c:Community{name: 'Database Community'});
UNWIND ['Data Management', 'Indexing', 'Big Data', 'Data Processing','Data Storage', 'Data Querying'] AS keywordName
MATCH (k:Keyword {keyword:keywordName})
MATCH (c:Community{name: 'Database Community'})
MERGE (c)-[:CONTAINS]->(k);


// Find journals and conferences related to Database Community
MATCH (k1:Keyword)<-[:HAS_KEYWORD]-(p:Paper)-[:PUBLISHED_IN]->(jc)
WHERE jc:Journal OR jc:Conference
OPTIONAL MATCH (dc:Community {name:"Database Community"})-[:CONTAINS]->(k1)
WITH 
    jc.name AS journalOrConference,
    p.paperId AS paperId,
    count(DISTINCT dc) AS isComm
WITH 
    journalOrConference,
    count(DISTINCT paperId) AS numPaperPublished,
    sum(isComm) AS numPaperPublishedComm,
    ((toFloat(sum(isComm)) / toFloat(count(DISTINCT paperId))) * 100) AS percentage
WHERE 
    toFloat(numPaperPublishedComm) > 0 AND percentage > 90
WITH 
    journalOrConference,
    numPaperPublished,
    numPaperPublishedComm,
    percentage
MATCH (entity {name: journalOrConference})
MATCH (dc:Community {name:"Database Community"})
MERGE (entity)-[:RELATED_TO]->(dc);


// Identify Top Papers of these Conference/Journal 
MATCH (p1:Paper)-[:PUBLISHED_IN]->(jc1)-[:RELATED_TO]->(dc:Community {name:"Database Community"})
MATCH (p2:Paper)-[:PUBLISHED_IN]->(jc2)-[:RELATED_TO]->(dc:Community {name:"Database Community"})
MATCH (p2)-[:CITES]->(p1)
WITH p1, count(distinct p2.paperId) AS citationCount
ORDER BY citationCount DESC
LIMIT 100
SET p1:TopDBCommPaper;


// Identify Potential Good Match and Gurus
MATCH (p:TopDBCommPaper)-[:WRITTEN_BY]->(a:Author)
MATCH (c:Community{name:"Database Community"})
MERGE (a)-[:POTENTIAL_REVIEWER]->(c);


MATCH (p:TopDBCommPaper)-[:WRITTEN_BY]->(a:Author)
MATCH (c:Community{name:"Database Community"})
WITH a,count(p) as cnt
WHERE cnt>=2
MERGE (a)-[:GURU]->(c);