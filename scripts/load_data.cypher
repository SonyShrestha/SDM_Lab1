// Load Data
// Delete everything 
MATCH(n) DETACH DELETE n; 


// Create node author
LOAD CSV FROM 'file:///authors.csv' AS  line FIELDTERMINATOR ','
with line
skip 1
CREATE (a:author {authorId: trim(line[0]), name: line[1]});


// Create node keywords
LOAD CSV FROM 'file:///keywords.csv' AS  line FIELDTERMINATOR ','
with line
skip 1
CREATE (k:keyword {keyword: trim(line[0])});


// Create node journal 
LOAD CSV FROM 'file:///journals.csv' AS  line FIELDTERMINATOR ','
with line
skip 1
CREATE (j:journal{name: trim(line[0])});


// Create node conference 
LOAD CSV FROM 'file:///conferences.csv' AS  line FIELDTERMINATOR ','
with line
skip 1
CREATE (c:conference{name: trim(line[0])});


// Create node workshop 
LOAD CSV FROM 'file:///workshops.csv' AS  line FIELDTERMINATOR ','
with line
skip 1
CREATE (w:workshop{name: trim(line[0])});


// Create node journal paper
LOAD CSV FROM 'file:///journal_paper.csv' AS  line FIELDTERMINATOR ','
with line
skip 1
CREATE (p:paper{paperId: trim(line[0]),title: trim(line[1]),abstract: trim(line[2]), 
citationCount: toInteger(line[4]),publicationDate: trim(line[5])});


// Create node conference paper
LOAD CSV FROM 'file:///conference_paper.csv' AS  line FIELDTERMINATOR ','
with line
skip 1
CREATE (p:paper{paperId: trim(line[0]),title: trim(line[1]),abstract: trim(line[2]), 
citationCount: toInteger(line[4]),publicationDate: trim(line[5])});


// Create node workshop paper
LOAD CSV FROM 'file:///workshop_paper.csv' AS  line FIELDTERMINATOR ','
with line
skip 1
CREATE (p:paper{paperId: trim(line[0]),title: trim(line[1]),abstract: trim(line[2]), 
citationCount: toInteger(line[4]),publicationDate: trim(line[5])});


// Create Relationship (Paper -> WRITTEN_BY -> Author) [FOR JOURNAL PAPER]
LOAD CSV FROM 'file:///journal_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (p:paper {paperId : trim(line[0])})
SET   
    p.title = trim(line[1]),
    p.abstract = trim(line[2]),
    p.citationCount = trim(line[4]),
    p.publicationDate = trim(line[5])
WITH p, split(line[8],',') AS authorIds
UNWIND authorIds AS aId
MERGE (a:author {authorId: trim(aId)}) 
MERGE (p)-[:WRITTEN_BY]->(a);


// Create Relationship (Paper -> WRITTEN_BY -> Author) [FOR CONFERENCE PAPER]
LOAD CSV FROM 'file:///conference_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (p:paper {paperId : line[0]})
SET   
    p.title = trim(line[1]),
    p.abstract = trim(line[2]),
    p.citationCount = trim(line[4]),
    p.publicationDate = trim(line[5])
WITH p, split(line[8],',') AS authorIds
UNWIND authorIds AS aId
MERGE (a:author {authorId: trim(aId)}) 
MERGE (p)-[:WRITTEN_BY]->(a);


// Create Relationship (Paper -> WRITTEN_BY -> Author) [FOR WORKSHOP PAPER]
LOAD CSV FROM 'file:///workshop_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (p:paper {paperId : trim(line[0])})
SET   
    p.title = trim(line[1]),
    p.abstract = trim(line[2]),
    p.citationCount = trim(line[4]),
    p.publicationDate = trim(line[5])
WITH p, split(line[8],',') AS authorIds
UNWIND authorIds AS aId
MERGE (a:author {authorId: trim(aId)}) 
MERGE (p)-[:WRITTEN_BY]->(a);


// Create Relationshio has keywords
// Create Relationship (Paper -> HAS_KEYWORD -> Author) [FOR JOURNAL PAPER]
LOAD CSV FROM 'file:///journal_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (p:paper {paperId : trim(line[0])})
SET   
    p.title = trim(line[1]),
    p.abstract = trim(line[2]),
    p.citationCount = trim(line[4]),
    p.publicationDate = trim(line[5])
WITH p, split(line[6],',') AS keywords
UNWIND keywords AS kId
MERGE (k:keyword {keyword: trim(kId)}) 
MERGE (p)-[:HAS_KEYWORD]->(k);


// Create Relationship (Paper -> HAS_KEYWORD -> Author) [FOR CONFERENCE PAPER]
LOAD CSV FROM 'file:///conference_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (p:paper {paperId : trim(line[0])})
SET   
    p.title = trim(line[1]),
    p.abstract = trim(line[2]),
    p.citationCount = trim(line[4]),
    p.publicationDate = trim(line[5])
WITH p, split(line[6],',') AS keywords
UNWIND keywords AS kId
MERGE (k:keyword {keyword: trim(kId)}) 
MERGE (p)-[:HAS_KEYWORD]->(k);


// Create Relationship (Paper -> HAS_KEYWORD -> Author) [FOR WORKSHOP PAPER]
LOAD CSV FROM 'file:///workshop_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (p:paper {paperId : trim(line[0])})
SET   
    p.title = trim(line[1]),
    p.abstract = trim(line[2]),
    p.citationCount = trim(line[4]),
    p.publicationDate = trim(line[5])
WITH p, split(line[6],',') AS keywords
UNWIND keywords AS kId
MERGE (k:keyword {keyword: trim(kId)}) 
MERGE (p)-[:HAS_KEYWORD]->(k);


// Create relationship Paper -> PUBLISHED_IN -> Journal
LOAD CSV FROM 'file:///journal_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (p:paper {paperId : trim(line[0])})
SET   
    p.title = trim(line[1]),
    p.abstract = trim(line[2]),
    p.citationCount = trim(line[4]),  
    p.publicationDate = trim(line[5])
WITH p, trim(line[7]) AS journalName, trim(line[12]) as journalVolume, toInteger(line[3]) as year
MERGE (j:journal {name: journalName}) 
MERGE (p)-[pin:PUBLISHED_IN]->(j)
SET pin.volume = journalVolume,
pin.year = year;


// Create relationship Paper -> PUBLISHED_IN -> Conference
LOAD CSV FROM 'file:///conference_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (p:paper {paperId : trim(line[0])})
SET   
    p.title = trim(line[1]),
    p.abstract = trim(line[2]),
    p.year = trim(line[3]),
    p.citationCount = trim(line[4]),  
    p.publicationDate = trim(line[5])
WITH p, trim(line[7]) AS conferenceName, trim(line[12]) as edition, toInteger(line[3]) as year
MERGE (c:conference {name: conferenceName}) 
MERGE (p)-[pin:PUBLISHED_IN]->(c)
SET pin.edition = edition,
pin.year = year;


// Create relationship Paper -> PUBLISHED_IN -> Workshop
LOAD CSV FROM 'file:///workshop_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (p:paper {paperId : trim(line[0])})
SET   
    p.title = trim(line[1]),
    p.abstract = trim(line[2]),
    p.citationCount = trim(line[4]),  
    p.publicationDate = trim(line[5])
WITH p, trim(line[7]) AS workshopName, trim(line[12]) as edition, toInteger(line[3]) as year
MERGE (w:workshop {name:  workshopName}) 
MERGE (p)-[pin:PUBLISHED_IN]->(w)
SET pin.edition = edition,
pin.year = year;


// Create Relationship Paper -> CITES -> Paper [Journal]
LOAD CSV FROM 'file:///journal_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (p:paper {paperId : trim(line[0])})
SET   
    p.title = trim(line[1]),
    p.abstract = trim(line[2]),
    p.citationCount = trim(line[4]),
    p.publicationDate = trim(line[5])
WITH p, split(line[11],',') AS citedPaperIds
UNWIND citedPaperIds AS citedPaperId
MERGE (p1:paper {paperId: trim(citedPaperId)}) 
MERGE (p)-[:CITES]->(p1);


// Create Relationship Paper -> CITES -> Paper [Conference]
LOAD CSV FROM 'file:///conference_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (p:paper {paperId : trim(line[0])})
SET   
    p.title = trim(line[1]),
    p.abstract = trim(line[2]),
    p.citationCount = trim(line[4]),
    p.publicationDate = trim(line[5])
WITH p, split(line[11],',') AS citedPaperIds
UNWIND citedPaperIds AS citedPaperId
MERGE (p1:paper {paperId: trim(citedPaperId)}) 
MERGE (p)-[:CITES]->(p1);



// Create Relationship Paper -> CITES -> Paper [Workshop]
LOAD CSV FROM 'file:///workshop_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (p:paper {paperId : trim(line[0])})
SET   
    p.title = trim(line[1]),
    p.abstract = trim(line[2]),
    p.citationCount = trim(line[4]),
    p.publicationDate = trim(line[5])
WITH p, split(line[11],',') AS citedPaperIds
UNWIND citedPaperIds AS citedPaperId
MERGE (p1:paper {paperId: trim(citedPaperId)}) 
MERGE (p)-[:CITES]->(p1);


// Create Relationship (Paper -> REVIEWED_BY -> Author) [FOR JOURNAL PAPER]
LOAD CSV FROM 'file:///journal_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (p:paper {paperId : trim(line[0])})
SET   
    p.title = trim(line[1]),
    p.abstract = trim(line[2]),
    p.citationCount = trim(line[4]),
    p.publicationDate = trim(line[5])
WITH p, split(line[13],',') AS authorIds
UNWIND authorIds AS aId
MERGE (a:author {authorId: trim(aId)}) 
MERGE (p)-[:REVIEWED_BY]->(a);


// Create Relationship (Paper -> REVIEWED_BY -> Author) [FOR CONFERENCE PAPER]
LOAD CSV FROM 'file:///conference_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (p:paper {paperId : trim(line[0])})
SET   
    p.title = trim(line[1]),
    p.abstract = trim(line[2]),
    p.citationCount = trim(line[4]),
    p.publicationDate = trim(line[5])
WITH p, split(line[13],',') AS authorIds
UNWIND authorIds AS aId
MERGE (a:author {authorId: trim(aId)}) 
MERGE (p)-[:REVIEWED_BY]->(a);


// Create Relationship (Paper -> REVIEWED_BY -> Author) [FOR WORKSHOP PAPER]
LOAD CSV FROM 'file:///workshop_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (p:paper {paperId : trim(line[0])})
SET   
    p.title = trim(line[1]),
    p.abstract = trim(line[2]),
    p.citationCount = trim(line[4]),
    p.publicationDate = trim(line[5])
WITH p, split(line[13],',') AS authorIds
UNWIND authorIds AS aId
MERGE (a:author {authorId: trim(aId)}) 
MERGE (p)-[:REVIEWED_BY]->(a);


// Create relationship Paper -> CORRESPONDING_AUTHOR -> Author [JOURNAL]
LOAD CSV FROM 'file:///journal_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (p:paper {paperId : trim(line[0])})
SET   
    p.title = trim(line[1]),
    p.abstract = trim(line[2]),
    p.citationCount = trim(line[4]),
    p.publicationDate = trim(line[5])
WITH p, trim(line[10]) as correnspondingAuthor
MERGE (a:author {authorId: correnspondingAuthor}) 
MERGE (p)-[:CORRESPONDING_AUTHOR]->(a);


// Create relationship Paper -> CORRESPONDING_AUTHOR -> Author [CONFERENCE]
LOAD CSV FROM 'file:///conference_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (p:paper {paperId : trim(line[0])})
SET   
    p.title = trim(line[1]),
    p.abstract = trim(line[2]),
    p.citationCount = trim(line[4]),
    p.publicationDate = trim(line[5])
WITH p, trim(line[10]) as correnspondingAuthor
MERGE (a:author {authorId: correnspondingAuthor}) 
MERGE (p)-[:CORRESPONDING_AUTHOR]->(a);


// Create relationship Paper -> CORRESPONDING_AUTHOR -> Author [WORKSHOP]
LOAD CSV FROM 'file:///workshop_paper.csv' AS line FIELDTERMINATOR ','
WITH line
SKIP 1
MERGE (p:paper {paperId : trim(line[0])})
SET   
    p.title = trim(line[1]),
    p.abstract = trim(line[2]),
    p.citationCount = trim(line[4]),
    p.publicationDate = trim(line[5])
WITH p, trim(line[10]) as correnspondingAuthor
MERGE (a:author {authorId: correnspondingAuthor}) 
MERGE (p)-[:CORRESPONDING_AUTHOR]->(a);