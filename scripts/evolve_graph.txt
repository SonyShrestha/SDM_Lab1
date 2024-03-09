// Change model to store the review sent by each reviewer along with suggested decision
MATCH ()-[r:REVIEWED_BY]->()
SET r.reviewContent = 'This paper has been reviewed', r.suggestedDecision = 'Approved';


// Create Organization Node and load data 
LOAD CSV FROM 'file:///organizations.csv' AS  line FIELDTERMINATOR ','
with line
skip 1
CREATE (o:organization{name: line[0],type: line[1]});


// Extend the model to store the affiliation of the author
LOAD CSV FROM 'file:///authors.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (a:author {authorId : line[0]})
SET   
    a.name= line[1]
MERGE (o:organization {name:line[2]}) 
MERGE (a)-[:AFFILIATED_TO]->(o);