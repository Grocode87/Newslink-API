"""
Helper module to return a json version of a cluster
"""
import operator
from .article_format import get_return_article

def top_article(cluster_stories):
    """
    Returns the top story from a cluster
    """
    if cluster_stories == cluster_stories:
        pass

    return 0

def get_json_cluster(clusterid, category, cursor):
    """
    Returns the json version of a cluster
    """
    cursor.execute("SELECT * FROM articles WHERE id IN (SELECT article_id FROM `cluster details` WHERE cluster_id = %s)", [clusterid])
    cluster_articles = cursor.fetchall()

    top_article_id = top_article(cluster_articles)
    
    other_articles = []
    all_top_entities = []
    for index, article in enumerate(cluster_articles):
        # get the json formatted version of the article based off the ID
        article_json = get_return_article(article, category, cursor)

        for entity in article_json["entities"]:
                all_top_entities.append(entity)

        if index != top_article_id:
            # not top article, add to other articles
            other_articles.append(article_json)
        else:
            # top article, set as top article
            top_article_json = article_json

    # find the most common entities in the cluster
    top_entity_dict = {}
    for entity in all_top_entities:
        if entity in top_entity_dict:
            top_entity_dict[entity] += 1
        else:
            top_entity_dict[entity] = 0

    top_entities_sorted = sorted(top_entity_dict.items(), key=operator.itemgetter(1), reverse=True)
    cluster_top_entities = [entity[0] for entity in top_entities_sorted[:2]]

    json_obj = {
        'cluster_id': clusterid,
        'top_article': top_article_json,
        'other_articles': other_articles,
        'cluster_entities': cluster_top_entities
    }

    return json_obj
