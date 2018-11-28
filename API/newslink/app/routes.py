"""
Define routes and import api functions
"""

from flask import jsonify, request
from app import app, db_connection

from .get_top.get_top import get_top_stories
from .autocomplete.autocomplete import get_autocomplete
from .search_stories.search_stories import get_all_search_stories
from .search_stories.search_stories import get_top_search_stories
from .related_entities.related_entities import get_related_entities
from .recommend_stories.recommend_stories import get_recommended_stories

from .helper.cors_headers import add_cors_headers

@app.route('/')
@app.route('/index')
def index():
    """
    Server index site
    """
    return jsonify({'title': 'NewsLink API', 'version':'Unreleased'})

@app.route('/get/top')
def route_get_top():
    """
    get the top stories, if a category is not included it gets all top stories
    """

    category = request.args.get('category')
    returned = get_top_stories(db_connection.cursor(), category=category)

    return jsonify(returned)

@app.route("/get/autocomplete/<query>")
def route_autocomplete_entities(query):
    """
    return autocomplete results for entity search
    """
    returned = get_autocomplete(db_connection.cursor(), query)

    return jsonify(returned)

@app.route("/get/search/<query>")
def route_get_query_articles(query):
    """
    return stories based on query
    """
    returned = get_all_search_stories(db_connection.cursor(), query)

    return jsonify(returned)

@app.route("/get/search/<query>/top")
def route_get_top_query_articles(query):
    """
    return top stories based on query
    """
    returned = get_top_search_stories(db_connection.cursor(), query)

    return jsonify(returned)

@app.route("/get/related/entities/<query>")
def route_get_related_entities(query):
    """
    return the entities most similar to the query
    """
    returned = get_related_entities(db_connection.cursor(), query, 20)

    return jsonify(returned)

@app.route("/get/recommended/stories")
def route_get_recommended_stories():
    """
    return recommended stories based on entities from previous stories
    """
    print("WARNING: Getting recommended stories using text file entites for testing")
    #entity_history = request.args.get('entities')
    entity_history = None

    returned = get_recommended_stories(db_connection.cursor(), entity_history, 20)

    return jsonify(returned)

@app.route("/get/recommended/tags")
def route_get_recommended_entities();
    """
    return recommended entites most similar to the entities the user already has
