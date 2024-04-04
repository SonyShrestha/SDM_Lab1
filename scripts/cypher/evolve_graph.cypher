// Evolution 1: Extend the model to store the review sent by each reviewer along with suggested decision
LOAD CSV FROM 'file:///conference_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
WITH trim(line[0]) AS paperId, split(line[13],',') AS reviewers, split(line[14], ',') AS reviews, split(line[15], ',') AS decisions
UNWIND range(0, size(reviewers) - 1) AS index
MERGE (p:Paper {paperId : paperId})
MERGE (a:Author {authorId: reviewers[index]})
MERGE (p)-[rb:REVIEWED_BY]->(a)
SET rb.reviewContent = reviews[index],
    rb.suggestedDecision = decisions[index];

LOAD CSV FROM 'file:///journal_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
WITH trim(line[0]) AS paperId, split(line[13],',') AS reviewers, split(line[14], ',') AS reviews, split(line[15], ',') AS decisions
UNWIND range(0, size(reviewers) - 1) AS index
MERGE (p:Paper {paperId : paperId})
MERGE (a:Author {authorId: reviewers[index]})
MERGE (p)-[rb:REVIEWED_BY]->(a)
SET rb.reviewContent = reviews[index],
    rb.suggestedDecision = decisions[index];

LOAD CSV FROM 'file:///workshop_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
WITH trim(line[0]) AS paperId, split(line[13],',') AS reviewers, split(line[14], ',') AS reviews, split(line[15], ',') AS decisions
UNWIND range(0, size(reviewers) - 1) AS index
MERGE (p:Paper {paperId : paperId})
MERGE (a:Author {authorId: reviewers[index]})
MERGE (p)-[rb:REVIEWED_BY]->(a)
SET rb.reviewContent = reviews[index],
    rb.suggestedDecision = decisions[index];



// Create organization node and load data 
LOAD CSV FROM 'file:///organizations.csv' AS  line FIELDTERMINATOR ','
with line
skip 1
CREATE (o:organization{name: line[0],type: line[1]});

// Evolution 2: Extend the model to store the affiliation of the author
LOAD CSV FROM 'file:///authors.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (a:author {authorId : line[0]})
SET   
    a.name= line[1]
MERGE (o:organization {name:line[2]}) 
MERGE (a)-[:AFFILIATED_TO]->(o);