from neo4j import GraphDatabase

# Define connection
def connect_to_database(uri, user, password, database=None):
    if database:
        return GraphDatabase.driver(uri, auth=(user, password), database=database)
    else:
        return GraphDatabase.driver(uri, auth=(user, password))

def main():
    # connectDB
    driver = connect_to_database("bolt://localhost:7687", "neo4j", "admin123")
    with driver.session() as session:
        
        # load author
        session.run('''
        LOAD CSV WITH HEADERS FROM "file:///authors.csv" AS rowAuthor
        CREATE (a:Author {id: toInteger(rowAuthor.authorid), name: rowAuthor.name, url: rowAuthor.ur});
        ''')
        # load paper
        session.run('''
        LOAD CSV WITH HEADERS FROM "file:///papers.csv" AS rowPaper
        CREATE (p:Paper {id: toInteger(rowPaper.corpusid), title: rowPaper.title, year: toInteger(rowPaper.year), url: rowPaper.url, openAcces: toBoolean(rowPaper.isopenaccess), publicationDate: date(rowPaper.publicationdate), updated: rowPaper.updated, DOI:rowPaper.DOI, PubMedCentral: rowPaper.PubMedCentral, PubMed:rowPaper.PubMed, DBLP: rowPaper.DBLP, ArXiv: rowPaper.ArXiv, ACL: rowPaper.ACL, MAG: rowPaper.MAG});
        ''')
        # load keywords
        session.run('''
        LOAD CSV WITH HEADERS FROM "file:///keywords.csv" AS rowKw
        CREATE (k:Keyword {keyword: rowKw.keyword});
        ''')
        # load journal
        session.run('''
        LOAD CSV WITH HEADERS FROM "file:///journals.csv" AS rowJournal
        CREATE (j:Journal {id: rowJournal.venueID, name: rowJournal.journalName, issn: rowJournal.issn, url: rowJournal.url});
        ''')
        # load volume
        session.run('''
        LOAD CSV WITH HEADERS FROM "file:///volume-from.csv" AS row
        MATCH (j:Journal {id:row.journalID})
        CREATE (j)<-[:VOLUME_FROM]-(v: Volume {id: row.volumeID, year: toInteger(row.year), volume: toInteger(row.volume)});
        ''')
        # load conference
        session.run('''
        LOAD CSV WITH HEADERS FROM "file:///conferences.csv" AS rowConference
        CREATE (c:Conference {id: rowConference.conferenceID, name: rowConference.conferenceName, issn: rowConference.issn, url: rowConference.url});
        ''')
        # load edition
        session.run('''
        LOAD CSV WITH HEADERS FROM "file:///is-from.csv" AS rowEdition
        MATCH (conference:Conference {id: rowEdition.conferenceID})
        CREATE (e:Edition {id: rowEdition.editionID, edition: toInteger(rowEdition.edition), startDate: date(rowEdition.startDate), endDate: date(rowEdition.endDate)})-[:IS_FROM]->(conference);
        ''')

        # load relation written-by
        session.run('''
        LOAD CSV WITH HEADERS FROM "file:///written-by.csv" AS rowRelation
        MATCH (author:Author {id: toInteger(rowRelation.authorID)})
        MATCH (paper:Paper {id: toInteger(rowRelation.paperID)})
        CREATE (paper)-[:WRITTEN_BY{corresponding_author: toBoolean(rowRelation.is_corresponding)}]->(author);
        ''')
        # load belongsto
        session.run('''
        LOAD CSV WITH HEADERS FROM "file:///belongs-to.csv" AS rowBelongs
        MATCH (paper:Paper {id: toInteger(rowBelongs.paperID)})
        MATCH (edition: Edition {id:rowBelongs.venueID})
        CREATE (paper)-[:BELONGS_TO]->(edition);
        ''')
        # load published
        session.run('''
        LOAD CSV WITH HEADERS FROM "file:///published-in.csv" AS row
        MATCH (volume:Volume {id: row.venueID})
        MATCH (paper:Paper {id: toInteger(row.paperID)})
        CREATE (paper)-[:PUBLISHED_IN { startPage: row.startPage, endPage: row.endPage} ]->(volume);
        ''')
        # load abstract
        session.run('''
        LOAD CSV WITH HEADERS FROM 'file:///withAbstracts.csv' AS rowAbstract
        MATCH (p:Paper {id: toInteger(rowAbstract.paperID)}) SET p.abstract=rowAbstract.abstract;
        ''')
        # load citedby
        session.run('''
        LOAD CSV WITH HEADERS FROM "file:///cited-by.csv" AS rowRel
        MATCH (p1:Paper {id: toInteger(rowRel.paperID_cited)})
        MATCH (p2:Paper {id: toInteger(rowRel.paperID_citing)})
        CREATE (p1)-[:CITED_BY]->(p2);
        ''')
        # load relatedto
        session.run('''
        LOAD CSV WITH HEADERS FROM "file:///related-to.csv" AS rowCategory
        MATCH (p1:Paper {id: toInteger(rowCategory.paperID)})
        MATCH (k:Keyword {keyword: rowCategory.keyword})
        CREATE (p1)-[:RELATED_TO]->(k);
        ''')

    driver.close()

if __name__ == "__main__":
    main()
