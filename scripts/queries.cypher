// Find the top 3 most cited papers of each conference
MATCH (p:Paper)-[:PUBLISHED_IN]->(c:Conference)
OPTIONAL MATCH (p1:Paper)-[:CITES]->(p)
WITH c, p.paperId AS paperId, count(distinct p1.paperId) AS citationCount
ORDER BY c.name ASC, citationCount DESC
WITH c, collect({paperId: paperId, citationCount: citationCount}) AS papersByConference
RETURN c.name AS ConferenceName, papersByConference[0..3] AS TopPapers;


// For each conference find its community: i.e., those authors that have published papers on that conference in, at least, 4 different editions
MATCH (p:Paper)-[:WRITTEN_BY]->(a:Author)
MATCH (p)-[pi:PUBLISHED_IN]->(c:Conference)
WITH a.authorId AS authorId, c.name AS conferenceName, COUNT(DISTINCT pi.edition) AS numDistinctEditions
WHERE numDistinctEditions >= 4
RETURN conferenceName, COLLECT(authorId) AS community;



// Find the impact factors of the journals in your graph 
MATCH (j:Journal)
WITH j.name AS journalName, 2021 AS year
OPTIONAL MATCH (last1_p:Paper)-[last1:PUBLISHED_IN{year:year-1}]->(j:Journal{name:journalName})
WITH journalName,year,collect(last1_p.paperId) as paperId1
OPTIONAL MATCH (last2_p:Paper)-[last2:PUBLISHED_IN{year:year-2}]->(j:Journal{name:journalName})
WITH journalName, year,paperId1,collect(last2_p.paperId) as paperId2
with journalName, year, paperId1+paperId2 as paperIds,size(paperId1)+size(paperId2) as numPaperPublished_last2yrs
UNWIND paperIds AS paperId
with journalName,year, paperId, numPaperPublished_last2yrs
MATCH (p1:Paper)-[:CITES]->(p2:Paper{paperId:paperId})
MATCH (p1)-[:PUBLISHED_IN{year:year}]->(j:Journal)
with journalName,year,numPaperPublished_last2yrs,count(*) AS numCitations
return journalName,year,numCitations,numPaperPublished_last2yrs,numCitations/numPaperPublished_last2yrs AS impact_factor;


// Find the h-indexes of the authors in your graph
MATCH (a:Author)<-[:WRITTEN_BY]-(p:Paper)
OPTIONAL MATCH (p)<-[:CITES]-(citingPaper:Paper)
WITH a, p, COUNT(citingPaper) AS citations
ORDER BY a.name, citations DESC
WITH a, COLLECT(citations) AS citationCounts
WITH a, citationCounts, RANGE(1, SIZE(citationCounts)) AS indices
UNWIND indices AS idx
WITH a.name AS authorName, idx AS h, citationCounts
WHERE citationCounts[idx-1] >= idx
RETURN authorName, MAX(h) AS hIndex
ORDER BY hIndex DESC