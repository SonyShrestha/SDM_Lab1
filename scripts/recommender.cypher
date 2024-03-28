// Create node Commnity of type Database 
CREATE (c:Commnity{name: 'Database Community'});
UNWIND ['Data Management', 'Indexing', 'Big Data', 'Data Processing','Data Storage', 'Data Querying'] AS keywordName
MATCH (k:Keyword {keyword:keywordName})
MATCH (c:Commnity{name: 'Database Community'})
MERGE (c)-[:CONTAINS]->(k);

// Find journals and conferences related to Database Community
MATCH (p:Paper)-[:PUBLISHED_IN]->(jc)
WHERE jc:Journal OR jc:Conference
OPTIONAL MATCH (p)-[:HAS_KEYWORD]->(k1:Keyword)
OPTIONAL MATCH (dc:Community {name:"Database Community"})-[:CONTAINS]->(k2:Keyword{keyword:k1.keyword})
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
MATCH (entity {name: journalOrConference}), (dc:Community {name:"Database Community"})
MERGE (entity)-[:RELATED_TO]->(dc);


// Identify Top Papers of these Conference/Journal 
MATCH (p1:Paper)-[:PUBLISHED_IN]->(jc1)
MATCH (p2:Paper)-[:PUBLISHED_IN]->(jc2)
MATCH (jc1)-[:RELATED_TO]->(dc:Community)
MATCH (jc2)-[:RELATED_TO]->(dc:Community)
MATCH (p2:Paper)-[:CITES]->(p1:Paper)
WITH jc1, p1.paperId AS paper_Id, count(distinct p2.paperId) AS citationCount
ORDER BY jc1.name ASC, citationCount DESC
WITH jc1.name AS conferenceName, collect({paperId: paper_Id, citationCount: citationCount}) AS papersByConference
WITH conferenceName AS ConferenceName, REDUCE(acc = [], paperData IN papersByConference | acc + paperData)[0..100] AS TopPapers
UNWIND TopPapers AS paper
MATCH (pm:Paper{paperId:paper.paperId})
SET pm:TopDBCommpaper;


// Identify Potential Good Match and Gurus
MATCH (p:TopDBCommpaper)-[:WRITTEN_BY]->(a:Author)
MATCH (c:Community{name:"Database Community"})
MERGE (a)-[:POTENTIAL_REVIEWER_FOR]-(c);


MATCH (p:TopDBCommpaper)-[:WRITTEN_BY]->(a:Author)
MATCH (c:Community{name:"Database Community"})
WITH a,count(p) as cnt
WHERE cnt>=2
MERGE (a)-[:GURU_FOR]-(c);