from neo4j import GraphDatabase

# Define connection
def connect_to_database(uri, user, password, database=None):
    if database:
        return GraphDatabase.driver(uri, auth=(user, password), database=database)
    else:
        return GraphDatabase.driver(uri, auth=(user, password))

def main():

    driver = connect_to_database("bolt://localhost:7687", "neo4j", "upcsdm2024", database="testdb")
    
    with driver.session() as session:
        # 1. Add reviews
        session.run('''
            LOAD CSV WITH HEADERS FROM "file:///generated_reviews.csv" AS rowReview
            MATCH (p:Paper {id: toInteger(rowReview.paperID)})
            MATCH (a:Author {id: toInteger(rowReview.reviewerID)})
            MERGE (a)-[r:REVIEWED_BY]->(p)
            SET r.accepted = toInteger(rowReview.accepted),
                r.description = rowReview.review,
                p.paperID = toInteger(rowReview.paperID),
                r.reviewerID = toInteger(rowReview.reviewerID),
                r.accepted = toInteger(rowReview.accepted),
                r.review = rowReview.review
        ''')
        # 2. Update accepted_status 
        # 2.1 Init accepted_status = false
        session.run('''
            MATCH (p:Paper)
            SET p.accepted_status = false
        ''')
        session.run('''
            MATCH (p:Paper)
            OPTIONAL MATCH (p)-[e:REVIEWED_BY]->(:Author)
            WITH p, COUNT(e) AS numRev, SUM(CASE WHEN e.accepted = 1 THEN 1 ELSE 0 END) AS numAccepted
            WHERE rand() < 0.5 
            SET p.accepted_status = true
        ''')


        # 3. Adding affiliations
        session.run('''
            CREATE CONSTRAINT companyIdConstraint FOR (organization:Organization) REQUIRE organization.id IS UNIQUE;
            ''')
        # Load university data (generated randomly)
        session.run('''
            LOAD CSV WITH HEADERS FROM "file:///universities.csv" AS rowUniversity
            CREATE (u:Organization {id: rowUniversity.universityid, name:rowUniversity.university, type:'university'});
            ''')
        # Load companiy data (generated randomly)
        session.run('''
            LOAD CSV WITH HEADERS FROM "file:///companies.csv" AS rowCompany
            CREATE (c:Organization {id: rowCompany.companyid, name:rowCompany.company, type:'company'});
            ''')
        # Load affiliated data (generated randomly)
        session.run('''
            LOAD CSV WITH HEADERS FROM "file:///affiliated-to.csv" AS rowAffiliated
            MATCH(a:Author{id:toInteger(rowAffiliated.authorID)})
            MATCH(o:Organization{id:rowAffiliated.affiliation})
            CREATE (a)-[:IS_AFFILIATED_TO]->(o);
            ''')

    driver.close()

if __name__ == "__main__":
    main()
