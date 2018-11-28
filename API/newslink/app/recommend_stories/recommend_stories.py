"""
Module that recommends stories based on previous stories pressed
"""
import operator

from .ranking.basic_ranker import rank
from .sorting.basic_sorter import sort

from ..helper.random_clusters import get_random_clusters
from ..helper.cluster_format import get_json_cluster

# needed inputs

# cursor
# list of past x (1000+) entites
#   Format: (name, score), (name, score), ...
# (Possibly), tags that you are subscribed to

# number of random clusters to return that will be analyzed
SAMPLE_SIZE = 200

def get_recommended_stories(cursor, entity_history, num_clusters):
    """
    Function returns recommended stories
    """
    # temperarily get entities
    with open('app/recommend_stories/entities.txt', 'r') as file:
        articles = file.read().split("\n\n")
        entity_history = [(e.rsplit(',', 1)[0], e.rsplit(',', 1)[1]) for article in articles for e in article.split("\n")[1:]]


    # sort the entities
    sorted_entities = sort(entity_history)

    # get random clusters
    random_clusters = get_random_clusters(cursor, SAMPLE_SIZE)

    # rank the clusters
    scored_clusters = []
    for cluster in random_clusters:
        score = rank(cluster, sorted_entities, cursor)
        scored_clusters.append((cluster, score))

    # sort from highest to lowest rank
    scored_clusters.sort(key=operator.itemgetter(1), reverse=True)

    # keep the top x clusters
    top_clusters = scored_clusters[:num_clusters]
    top_cluster_ids = [cluster[0] for cluster in top_clusters]

    cursor.execute('SELECT * from clusters WHERE id IN %s ORDER BY rank DESC LIMIT 10', [top_cluster_ids])
    return_object = {'stories' : []}
    for id, cluster in enumerate(cursor.fetchall()):
        cluster_id = cluster[0]
        cluster_category = cluster[2]
        similarity_score = top_clusters[id][1]

        json_cluster = get_json_cluster(cluster_id, cluster_category, cursor)
        json_cluster['category'] = cluster_category
        json_cluster['similarity score'] = similarity_score

        return_object['stories'].append(json_cluster)

    return return_object
