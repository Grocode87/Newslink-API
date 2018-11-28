"""
Sorter - take the whole list of entities and sort the entities in a way that
         represents how much the user likes them

Basic sorter that simply adds up every entities scores and the highest ones
are sorted first
"""

import operator

def sort(entities):
    """
    Sort function - takes in the entity list and returns the sorted entities - top first
    """
    entity_scores = {}
    for entity_name, entity_rank in entities:
        if entity_name in entity_scores:
            entity_scores[entity_name] += float(entity_rank)
        else:
            entity_scores[entity_name] = float(entity_rank)

    # sort by score, highest first
    sorted_entities = [sorted(entity_scores.items(), key=operator.itemgetter(1), reverse=True)]

    return sorted_entities
