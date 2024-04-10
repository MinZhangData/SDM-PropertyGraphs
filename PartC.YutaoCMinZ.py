from neo4j import GraphDatabase
from config import *
import pandas as pd

driver = GraphDatabase.driver("bolt://localhost:7687", auth=(USER, PASSWORD))

with driver.session() as session:
    # C1 : Find/define the research communities
    result = session.run('''
        CREATE CONSTRAINT communityNameConstraint IF NOT EXISTS FOR (c:Community) REQUIRE c.name IS UNIQUE;
        ''')
    try:
        df = pd.DataFrame([dict(record) for record in result])
        print(df)
    except pd.errors.EmptyDataError:
        print("result is empty!")

    result = session.run('''
        MATCH (comm:Community {name: 'database'})-[r]-()
        DELETE r;
        ''')
    try:
        df = pd.DataFrame([dict(record) for record in result])
        print(df)
    except pd.errors.EmptyDataError:
        print("result is empty!")

    result = session.run('''
        MATCH (comm:Community {name: 'database'})
        MATCH (k:Keyword)
        WHERE k.keyword IN ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying']
        CREATE (comm)-[:DEFINED_BY]->(k);
        ''')
    try:
        df = pd.DataFrame([dict(record) for record in result])
        print(df)
    except pd.errors.EmptyDataError:
        print("result is empty!")





    # C2 : Find the conferences and journals related to the database community
    result = session.run('''
        MATCH (paper:Paper)-[:BELONGS_TO|PUBLISHED_IN]->(edition)-[:VOLUME_FROM|IS_FROM]->(venue)
        WITH venue.id AS venueId, COUNT(paper) AS numPapers, venue
        OPTIONAL MATCH (venue)-[rel:IN_COMMUNITY]->()
        DELETE rel
        WITH venueId, numPapers
        MATCH (community:Community)-[:DEFINED_BY]->(keyword)<-[:RELATED_TO]-(paper:Paper)-[:PUBLISHED_IN|BELONGS_TO]->(edition)-[:VOLUME_FROM|IS_FROM]->(relatedVenue)
        WHERE relatedVenue.id = venueId
        WITH relatedVenue, COUNT(DISTINCT paper) AS numPapersWithKeywords, community, numPapers
        WITH relatedVenue, numPapersWithKeywords, community, (toFloat(numPapersWithKeywords) / numPapers) AS percentage, numPapers
        WHERE percentage >= 0.6
        MERGE (relatedVenue)-[:IN_COMMUNITY]->(community);
                ''')
    try:
        df = pd.DataFrame([dict(record) for record in result])
        print(df)
    except pd.errors.EmptyDataError:
        print("result is empty!")

    # C3: Identify the top papers of these conferences/journals
    result = session.run('''
        MATCH (paper:Paper)-[:PUBLISHED_IN|BELONGS_TO]->(publication)-[:VOLUME_FROM|IS_FROM]->(venue)-[:IN_COMMUNITY]->(community:Community {name: 'database'})
        WITH paper, SIZE([(paper)-[:CITED_BY]->(:Paper) | 1]) AS citations
        ORDER BY citations DESC
        LIMIT 100
        SET paper.is_database_com_top = true
        RETURN paper.title, citations;
                ''')
    try:
        df = pd.DataFrame([dict(record) for record in result])
        print(df)
    except pd.errors.EmptyDataError:
        print("result is empty!")

    # C4: Find a potentially good match to review database papers and identify gurus
    result = session.run('''
        MATCH (author:Author)<-[:WRITTEN_BY]-(paper:Paper {is_database_com_top: true})
        SET author.potential_database_com_rev = true
        WITH author
        MATCH (author)<-[:WRITTEN_BY]-(topPaper:Paper {is_database_com_top: true})
        WITH author, COUNT(topPaper) AS papersCount
        WHERE papersCount >= 2
        SET author.database_com_guru = true
        RETURN author.name, papersCount, author.potential_database_com_rev, author.database_com_guru;
                ''')
    try:
        df = pd.DataFrame([dict(record) for record in result])
        print(df)
    except pd.errors.EmptyDataError:
        print("result is empty!")
    session.close()
