"""
Helper module to perform the task of getting random clusters from the database
"""

def get_random_clusters(cursor, amount):
    """
    Return x amount random clusters
    """
    cursor.execute('SELECT id FROM clusters ORDER BY RAND() LIMIT 0,%s', (amount,))
    clusters = [cluster[0] for cluster in cursor.fetchall()]

    return clusters