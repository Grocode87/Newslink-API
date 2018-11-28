"""
Helper module to convert an article into a json object
"""

def get_entities(cur, articleid):
    """
    Returns an article's sorted entities
    """
    cur.execute("""SELECT entities.name, entity_details.score
                FROM entity_details
                INNER JOIN entities ON entity_details.entity_id = entities.id
                WHERE entity_details.article_id = %s""", [articleid])
    return sorted(cur.fetchall(), key=lambda tup: tup[1], reverse=True)

def get_return_article(article_obj, article_category, cur):
    """
    Return a json interpretation of an article
    """
    time_uploaded_ago = "Not implemented"

    all_entities = [e[0] for e in get_entities(cur, article_obj[0])]
    top_entities = all_entities[:2]

    return_obj = {
        'article_id': article_obj[0],
        'title': article_obj[1],
        'desc': article_obj[2][:60],
        'url': article_obj[4],
        'image_url': article_obj[5],
        'category': article_category,
        'time_uploaded_ago': time_uploaded_ago,
        'source': article_obj[8],
        'entities': top_entities,
        'all_entities': all_entities
    }

    return return_obj
