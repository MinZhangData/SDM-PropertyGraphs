from neo4j import GraphDatabase
from config import *
import pandas as pd

driver = GraphDatabase.driver("bolt://localhost:7687", auth=(USER, PASSWORD))

with driver.session() as session:
    # Find the top 3 most cited papers of each conference.
    result = session.run('''
        MATCH (c:Conference)<-[:IS_FROM]-(e:Edition)<-[:BELONGS_TO]-(p:Paper)
        WITH c, p, size([(p)-[:CITED_BY]->(cited) WHERE cited:Paper | cited]) AS citations
        ORDER BY c, citations DESC
        WITH c, COLLECT({ id: p.id, title: p.title, citations: citations }) AS allPapers
        RETURN c.id AS ConferenceID, c.name AS ConferenceName, allPapers[0..3] AS TopCitedPapers
        ''')
    try:
        df = pd.DataFrame([dict(record) for record in result])
        print(df)
    except pd.errors.EmptyDataError:
        print("result is empty!")

    # For each conference find its community
    result = session.run('''
        MATCH (author:Author)-[:WRITTEN_BY]->(:Paper)-[:BELONGS_TO]->(edition:Edition)-[:IS_FROM]->(conf:Conference)
        WITH conf, author, COUNT(DISTINCT edition) AS editions
        WHERE editions >= 4
        WITH conf.name AS Conference, COLLECT(author.name) AS CommunityAuthors
        RETURN Conference, CommunityAuthors, SIZE(CommunityAuthors) AS NumberOfAuthors
                ''')
    try:
        df = pd.DataFrame([dict(record) for record in result])
        print(df)
    except pd.errors.EmptyDataError:
        print("result is empty!")

    #
    result = session.run('''
        MATCH (journal:Journal)<-[:VOLUME_FROM]-(vol:Volume)<-[:PUBLISHED_IN]-(paper:Paper)
        WITH journal, toInteger(vol.year) AS publicationYear, COUNT(paper) AS citationsCount
        OPTIONAL MATCH (journal)<-[:VOLUME_FROM]-(volPreviousYear:Volume{year: publicationYear - 1})<-[relPublishedInPrevYear: PUBLISHED_IN]-(:Paper)
        WITH journal, publicationYear, citationsCount, COUNT(relPublishedInPrevYear) AS publicationsLastYear
        OPTIONAL MATCH (journal)<-[:VOLUME_FROM]-(volTwoYearsAgo:Volume{year: publicationYear - 2})<-[relPublishedInTwoYearsAgo: PUBLISHED_IN]-(:Paper)
        WITH journal, publicationYear, citationsCount, publicationsLastYear, COUNT(relPublishedInTwoYearsAgo) AS publicationsTwoYearsAgo
        RETURN journal.id AS JournalIdentifier, publicationYear, citationsCount, publicationsLastYear, publicationsTwoYearsAgo,
               CASE publicationsLastYear + publicationsTwoYearsAgo WHEN 0 THEN 0.0 ELSE toFloat(citationsCount) / (publicationsLastYear + publicationsTwoYearsAgo) END AS ImpactFactor
        ORDER BY JournalIdentifier, publicationYear
                ''')
    try:
        df = pd.DataFrame([dict(record) for record in result])
        print(df)
    except pd.errors.EmptyDataError:
        print("result is empty!")

    result = session.run('''
        MATCH (a:Author)<-[r:WRITTEN_BY]-(p:Paper)
        OPTIONAL MATCH (p)-[c:CITED_BY]->(:Paper)
        WITH a, p, COUNT(c) AS citations
        ORDER BY a.name, citations DESC
        WITH a.name AS authorName, COLLECT(citations) AS citationsList
        UNWIND range(1, SIZE(citationsList)) AS idx
        WITH authorName, citationsList, idx
        WHERE citationsList[idx-1] >= idx
        WITH authorName, COLLECT(idx) AS validHIndex
        RETURN authorName, REDUCE(s = 0, i IN validHIndex | CASE WHEN i > s THEN i ELSE s END) AS h_index
        ORDER BY authorName
                ''')
    try:
        df = pd.DataFrame([dict(record) for record in result])
        print(df)
    except pd.errors.EmptyDataError:
        print("result is empty!")
    session.close()
