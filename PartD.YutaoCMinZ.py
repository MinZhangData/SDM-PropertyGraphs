from neo4j import GraphDatabase
from config import *
import pandas as pd

driver = GraphDatabase.driver("bolt://localhost:7687", auth=(USER, PASSWORD))

with driver.session() as session:
    # D1
    result = session.run('''
        CALL gds.graph.project(
          'Big_Graph1',
          ['Author', 'Paper', 'Edition', 'Conference'], 
          {
            RELATIONSHIP_TYPE_1: { 
              type: 'BELONGS_TO',
              orientation: 'REVERSE'
            },
            RELATIONSHIP_TYPE_2: { 
              type: 'WRITTEN_BY',
              orientation: 'NATURAL'
            },
            RELATIONSHIP_TYPE_3: { 
              type: 'IS_FROM',
              orientation: 'NATURAL'
            },
            RELATIONSHIP_TYPE_4: { 
              type: 'REVIEWED_BY',
              orientation: 'NATURAL'
            },
            RELATIONSHIP_TYPE5: { 
              type: 'CITED_BY',
              orientation: 'NATURAL'
            }
          }
        )
        ''')
    try:
        df = pd.DataFrame([dict(record) for record in result])
        print(df)
    except pd.errors.EmptyDataError:
        print("result is empty!")

    result = session.run('''
        CALL gds.closeness.stream('Big_Graph1')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).id AS id, score
        ORDER BY score DESC
        ''')
    try:
        df = pd.DataFrame([dict(record) for record in result])
        print(df)
    except pd.errors.EmptyDataError:
        print("result is empty!")





    # D2
    result = session.run('''
        CALL gds.graph.project(
          'Paper_Keyword', 
          ['Keyword', 'Paper'], 
          {
            WRITTEN_BY: { 
              type: 'RELATED_TO', 
              orientation: 'NATURAL' 
            }
          }
        )
                ''')
    try:
        df = pd.DataFrame([dict(record) for record in result])
        print(df)
    except pd.errors.EmptyDataError:
        print("result is empty!")


    result = session.run('''
        CALL gds.nodeSimilarity.stream('Paper_Keyword')
        YIELD node1, node2, similarity
        RETURN gds.util.asNode(node1).id AS ID1, gds.util.asNode(node2).id AS ID2, similarity
        ORDER BY similarity DESCENDING, ID1, ID2
                ''')
    try:
        df = pd.DataFrame([dict(record) for record in result])
        print(df)
    except pd.errors.EmptyDataError:
        print("result is empty!")


    session.close()
