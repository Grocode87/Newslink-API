"""
Module to handle autocomplete
"""

def get_autocomplete(cursor, query):
    """
    Return json list of autocomplete entities
    """
    cursor.execute("SELECT * FROM entities WHERE name LIKE %s ORDER BY total_occurences DESC LIMIT 9;", [query + "%"])
    return_obj = {'entities':[]}

    for entity in cursor.fetchall():
        return_obj['entities'].append({
            'name': entity[1],
            'score': entity[2]
        })
    return return_obj
