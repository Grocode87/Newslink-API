"""
Module to handle searching for stories
"""
from ..helper.cluster_format import get_json_cluster

def get_stories_title(cursor, query):
    """
    Get the story ids with the query in the title
    """
    story_cluster_ids = []

    cursor.execute("SELECT cluster_id FROM `cluster details` WHERE article_id IN (SELECT id FROM articles WHERE title LIKE %s) LIMIT 15;", ["%"+query+"%"])
    for row in cursor.fetchall():
        story_cluster_ids.append(row[0])

    return story_cluster_ids

def get_stories_entities(cursor, query, limit_results=True):
    """
    Get the story ids with the query in the entities

    TODO: Have to find out an efficient and effective way to do this
    """
    story_cluster_ids = []

    # get the id of the entity from the name
    cursor.execute("SELECT id FROM entities WHERE name=%s", (query,))
    entity_id = cursor.fetchall()

    if len(entity_id) > 0:
        # get the articles with the entity
        cursor.execute("SELECT article_id FROM entity_details where entity_id=%s", (entity_id[0][0],))
        article_ids = cursor.fetchall()

        # determine which articles have the entity in the top 2
        for article_id in article_ids:
            # if 10 articles have been found, break
            if limit_results and len(story_cluster_ids) > 10:
                break

            # get the top 2 entities of the article to see if the query is one of them
            cursor.execute("SELECT entity_id, score FROM entity_details WHERE article_id=%s ORDER BY score DESC LIMIT 2", (article_id[0],))
            for top_2_entity in cursor.fetchall():
                if top_2_entity[0] == entity_id[0][0]:
                    # get the cluster id the article is from and add it to list
                    cursor.execute("SELECT cluster_id FROM `cluster details` WHERE article_id=%s", (article_id[0],))
                    for row in cursor.fetchall():
                        if not row[0] in story_cluster_ids:
                            story_cluster_ids.append(row[0])

    return story_cluster_ids

def get_all_search_stories(cursor, query):
    """
    Function to get search stories
    """
    # Create list of cluster ids with different methods
    all_cluster_ids = list([0] +
                           get_stories_title(cursor, query) +
                           get_stories_entities(cursor, query))

    # select the clusters based off the list of ids
    cursor.execute('SELECT * from clusters WHERE id IN %s', [all_cluster_ids])
    return_object = {'stories' : []}
    for cluster in cursor.fetchall():
        cluster_id = cluster[0]
        cluster_category = cluster[2]
        cluster_rank = cluster[3]

        json_cluster = get_json_cluster(cluster_id, cluster_category, cursor)
        json_cluster['category'] = cluster_category
        json_cluster['rank'] = cluster_rank

        return_object['stories'].append(json_cluster)

    return return_object

def get_top_search_stories(cursor, query):
    """
    Function to get search stories
    """
    # Create list of cluster ids with different methods
    all_cluster_ids = list([0] +
                           get_stories_title(cursor, query))

    # select the clusters based off the list of ids
    cursor.execute('SELECT * from clusters WHERE id IN %s ORDER BY rank DESC LIMIT 10', [all_cluster_ids])
    return_object = {'stories' : []}
    for cluster in cursor.fetchall():
        cluster_id = cluster[0]
        cluster_category = cluster[2]
        cluster_rank = cluster[3]

        json_cluster = get_json_cluster(cluster_id, cluster_category, cursor)
        json_cluster['category'] = cluster_category
        json_cluster['rank'] = cluster_rank

        return_object['stories'].append(json_cluster)

    return return_object
