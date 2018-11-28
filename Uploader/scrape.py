"""
Helper module that contains logic for getting articles and scraping them
"""

import datetime
import json
import math
import pickle
import string

import pandas as pd
import requests

from newspaper import Article
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.corpus import wordnet as wn
from nltk.stem.porter import PorterStemmer
from sklearn.preprocessing import LabelEncoder


class ArticleGetter():
    """
    Class that retrives articles, either from a json file
    for testing, or the newsapi.org api for prod
    """
    def __init__(self):
        self._retrieval_url = "https://newsapi.org/v2/everything?language={0}&pageSize={1}&apiKey={2}&sources=".format( # pylint: disable=line-too-long
            "en", '100', '65cf3ce545bf41b0a3c5e2811cb7a04f')

    def get_articles_temp(self, from_index, to_index):
        """
        Getting articles from a json file for testing purposes
        """
        all_articles = []
        with open("../clustering_dataset.json", 'r') as file:
            json_data = json.load(file)

        # create article list to return
        for article in json_data:
            all_articles.append({
                'title': json_data[article]['title'],
                'text': json_data[article]['text'],
                'url': json_data[article]['url'],
                'source': json_data[article]['source'],
                'img_url': json_data[article]['img_url'],
                'entities': json_data[article]['entities']
            })
        return all_articles[from_index: to_index]

    def get_split_sources(self):
        """
        Split the sources and split them into lists of 20
        """
        # get all the sources from a text file
        with open("sources.txt", 'r') as file:
            sources = file.readlines()

        split_sources = []
        for i in range(int(math.ceil((len(sources) / 20)))):
            sources_temp = sources[i * 20: i * 20 + 20]
            source_string = ""
            for source in sources_temp:
                if source_string != "":
                    source_string += ","
                source_string += source.replace("\n", '')

            split_sources.append(source_string)
        return split_sources

    def get_articles(self, minutes_from):
        """
        Retrieve articles from the past x minutes
        """
        # get the datetime to get articles from
        # format it by removing the seconds
        iso_time = (datetime.datetime.utcnow()-datetime.timedelta(minutes=minutes_from)).isoformat()
        last_dot_index = str(iso_time).rfind(".")
        iso_time = iso_time[:last_dot_index]

        articles = []
        for sources in self.get_split_sources():
            keep_going = True
            index = 1
            # while the results are equal to the page size, keep requesting articles
            while keep_going:
                req = requests.get(self._retrieval_url + sources +
                                   "&page={0}&from={1}".format(str(index), iso_time))

                articles.extend(req.json()['articles'])

                if len(req.json()['articles']) < 100:
                    keep_going = False

                print(index)
                index += 1
        print("Total articles retrieved:", len(articles))
        return articles

class Scraper():
    """
    Class that contains the logic to scrape articles
    """
    column_names = ['id', 'text', 'category']

    def __init__(self):
        news = pd.read_csv("classifier/training_dataset.csv", names=self.column_names)

        # load the label encoder to decode category numbers
        self.encoder = LabelEncoder()
        self.encoder.fit_transform(news['category'])

        # load the text classifer
        self.text_clf = open("classifier/nb_classifier.pkl", "rb")
        self.text_clf = pickle.load(self.text_clf)

        self.porter = PorterStemmer()

        # prevents odd nltk error
        # https://stackoverflow.com/questions/27433370/what-would-cause-wordnetcorpusreader-to-have-no-attribute-lazycorpusloader
        wn.ensure_loaded()

    def clean_text(self, text):
        """
        Remove stopwords from, tokenize, and stem the text
        """
        stop = stopwords.words('english') + list(string.punctuation)
        words = word_tokenize(text.lower())
        words = [self.porter.stem(w) for w in words if not w in stop]

        return ' '.join(words)

    def get_text(self, url):
        """
        Scrape the article from the url and return the text
        """
        try:
            article = Article(url, language='en')
            article.download()
            article.parse()

            content = str.join(" ", article.text.splitlines())
        except:
            # handle any exceptions
            print("Error occured when getting text")
            content = ""

        return content

    def get_entities(self, text):
        """
        Use wikifier.org to the get the entities from text
        """
        entities = {}
        try:
            data = {
                "userKey": "jzanfsvrolfwraokwpxhxiatovhvyp",
                "text": text, "lang": 'en',
                "pageRankSqThreshold": "%g" % 0.9, "applyPageRankSqThreshold": "true",
                "nTopDfValuesToIgnore": "200",
                "wikiDataClasses": "true", "wikiDataClassIds": "false",
                "support": "true", "ranges": "false",
                "includeCosines": "false", "maxMentionEntropy": "2.1"
            }
            url = "http://www.wikifier.org/annotate-article"

            req = requests.post(url, data=data)
            response = req.json()

            for annotation in response['annotations']:
                entities[annotation['title']] = annotation['pageRank']
        except:
            # handle any exceptions
            print("Error occured when getting entities")
            entities = {}

        return entities

    def get_category(self, text):
        """
        Use the text classifier to get the category of the text
        """
        prediction = self.text_clf.predict([text])
        predicted_category = self.encoder.inverse_transform(prediction[0])

        return predicted_category

    def get_image_url(self, url):
        """
        Scrape the images from the url
        """
        article = Article(url, language='en')
        article.download()
        article.parse()

        return article.top_image
