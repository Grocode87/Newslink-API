"""
Ranker - Gives a cluster a score based on how similar it is to what the user likes

Basic ranker where the entity scores are added up if
they exist in the cluster

NOTE: Must be fast because it has to be run for every cluster
"""
import random

def rank(cluster_id, sorted_entities, cursor):
    """
    Method where ranker is implemented
    """
    cluster_score = 0

    # get the cluster entities (probably just from a random one of the articles)
    cursor.execute("SELECT article_id from `cluster details` WHERE cluster_id=%s", (cluster_id,))
    articles = cursor.fetchall()

    ran_article = random.randint(0, len(articles) - 1)
    cursor.execute("SELECT * FROM entities WHERE id IN (SELECT entity_id FROM entity_details WHERE article_id=%s)", (articles[ran_article][0],))

    entities = cursor.fetchall()

    for sort_ent in sorted_entities[0]:
        for ent1 in entities:
            if sort_ent[0] == ent1[1]:
                cluster_score += sort_ent[1] * ent1[0]

    return cluster_score
