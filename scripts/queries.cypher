// Find the top 3 most cited papers of each conference
MATCH (p:paper)-[:PUBLISHED_IN]->(c:conference)
MATCH (p1:paper)-[:CITES]->(p)
WITH c, p.paperId AS paperId, count(distinct p1.paperId) AS citationCount
ORDER BY c.name ASC, citationCount DESC
WITH c, collect({paperId: paperId, citationCount: citationCount}) AS papersByConference
RETURN c.name AS ConferenceName, papersByConference[0..3] AS TopPapers;


// For each conference find its community: i.e., those authors that have published papers on that conference in, at least, 4 different editions
MATCH (p:paper)-[:WRITTEN_BY]->(a:author)
MATCH (p)-[pi:PUBLISHED_IN]->(c:conference)
WITH a.authorId AS authorId, c.name AS conferenceName, COUNT(DISTINCT pi.edition) AS numDistinctEditions
WHERE numDistinctEditions >= 4
RETURN conferenceName, COLLECT(authorId) AS community;



// Find the impact factors of the journals in your graph 
MATCH (j:journal)
WITH j.name AS journalName, 2021 AS year
OPTIONAL MATCH (last1_p:paper)-[last1:PUBLISHED_IN{year:year-1}]->(j:journal{name:journalName})
OPTIONAL MATCH (last2_p:paper)-[last2:PUBLISHED_IN{year:year-2}]->(j:journal{name:journalName})
with journalName, year, collect(last1_p.paperId)+ collect(last2_p.paperId) as paperIds,count(distinct last1_p.paperId) + count(distinct last2_p.paperId) as numPaperPublished_last2yrs
UNWIND paperIds AS paperId
with journalName,year, paperId, numPaperPublished_last2yrs
MATCH (p1:paper)-[:CITES]->(p2:paper{paperId:paperId})
MATCH (p1)-[:PUBLISHED_IN{year:year}]->(j:journal)
with journalName,year,numPaperPublished_last2yrs,count(*) AS numCitations
return journalName,year,numCitations,numPaperPublished_last2yrs,numCitations/numPaperPublished_last2yrs AS impact_factor;


// Find the h-indexes of the authors in your graph
MATCH (a:author)<-[:WRITTEN_BY]-(p:paper)
OPTIONAL MATCH (p)<-[:CITES]-(citingPaper:paper)
WITH a, p, COUNT(citingPaper) AS citations
ORDER BY a.name, citations DESC
WITH a, COLLECT(citations) AS citationCounts
WITH a, citationCounts, RANGE(1, SIZE(citationCounts)) AS indices
UNWIND indices AS idx
WITH a.name AS authorName, idx AS h, citationCounts
WHERE citationCounts[idx-1] >= idx
RETURN authorName, MAX(h) AS hIndex
ORDER BY hIndex DESC
