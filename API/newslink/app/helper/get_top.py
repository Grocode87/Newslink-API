from ..helper.cluster_format import get_json_cluster

def get_top_stories(cursor, category=None):
    """
    Get the top stories with or without category
    """
    if category:
        # get specific clusters dependent on category
        cursor.execute("SELECT * FROM clusters WHERE last_updated > (NOW() - INTERVAL 24 HOUR) AND category=%s ORDER BY rank DESC LIMIT 12", [category])
    else:
        # get every top cluster
        cursor.execute("SELECT * from clusters WHERE last_updated > (NOW() - INTERVAL 24 HOUR) ORDER BY rank DESC LIMIT 12")

    return_object = {'content': {'stories' : []}}
    for cluster in cursor.fetchall():
        cluster_id = cluster[0]
        cluster_category = cluster[2]
        cluster_rank = cluster[3]

        json_cluster = get_json_cluster(cluster_id, cursor)
        json_cluster['category'] = cluster_category
        json_cluster['rank'] = cluster_rank

        return_object['content']['stories'].append(json_cluster)

    return cursor.fetchall()
        