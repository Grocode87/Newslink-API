"""
Get the entities most closely related to query
"""
import operator
import math

def get_related_entities(cursor, query, amount):
    """
    get related entities
    """
    # make sure the entity exists
    cursor.execute("SELECT id FROM entities WHERE name=%s", [query])
    if len(cursor.fetchall()) > 0:
        # get the ids of the articles that contain the query entity
        cursor.execute("SELECT article_id FROM entity_details WHERE entity_id = (SELECT id from entities WHERE name=%s)", [query])
        articles = cursor.fetchall()

        # convert the result above to a list
        list_of_ids = []
        for article in articles:
            list_of_ids.append(article[0])

        # create a string to help with formatting query below
        format_str = ','.join(['%s'] * len(list_of_ids))

        # Get the id, name, ttl_occurences, and count from every entity in the articles above
        cursor.execute("""
                        SELECT entities.id, entities.name, entities.total_occurences, COUNT(article_id) as count
                        FROM entities 
                        INNER JOIN 
                            (
                                SELECT entity_id, article_id from entity_details
                            ) ed 
                        ON ed.entity_id = entities.id 
                        WHERE ed.article_id IN (%s)
                        GROUP BY entities.id
                        ORDER BY count DESC
                       """ % format_str, list_of_ids)
        entities = cursor.fetchall()

        # get the total amount of articles for entity ranking
        cursor.execute("SELECT COUNT(*) FROM articles")
        total_articles = cursor.fetchall()[0][0]

        # create a lookup dict of the scores and names
        entity_scores = {}
        entity_names = {}
        for entity in entities:
            # calculate the entity score
            # ENTITY SCORE = ENTITY COUNT  * math.log(TOTAL ARTICLES / TOTAL ENTITY OCCURENCES)
            entity_scores[entity[0]] = entity[3] * math.log(total_articles / entity[2])

            entity_names[entity[0]] = entity[1]

        # sort the entities, highest score first
        entity_scores = sorted(entity_scores.items(), key=operator.itemgetter(1), reverse=True)[:amount]

        # create the object to return by getting the names of the entities
        return_obj = []
        for entity_id, _ in entity_scores:
            return_obj.append(entity_names[entity_id])

        return return_obj
    else:
        return {'error': 'this entity does not exist'}
