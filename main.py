import logging
from datetime import datetime

import requests
from mongoengine import (
    connect,
    Document,
    ObjectIdField,
    IntField,
    URLField,
    StringField,
    ListField,
    DateTimeField,
)
from mongoengine.errors import FieldDoesNotExist, NotUniqueError

import config

CRYPTOCOMPARE_URL = (
    "https://min-api.cryptocompare.com/data/v2/news/?lang=EN&api_key=key"
)


class Article(Document):
    _id = ObjectIdField()
    article_id = IntField()
    guid = URLField(unique=True)
    published_on = DateTimeField()
    imageurl = URLField()
    title = StringField()
    url = URLField()
    source = StringField()
    tags = StringField()
    body = StringField()
    categories = StringField()
    lang = StringField()
    source_info = ListField(StringField())
    date_created = DateTimeField(default=datetime.utcnow)


def create_article_from_dict(article_dict):
    i = article_dict    
    i["article_id"] = i.pop("id")
    try:
        del i["downvotes"]
        del i["upvotes"]
    except FieldDoesNotExist:
        pass

    i["source"] = i["source_info"]["name"]
    i["published_on"] = datetime.fromtimestamp(i["published_on"])

    article = Article(**i)
    return article


def main():
    logging.basicConfig(filename='c_news.log', level=logging.DEBUG)
    logging.info("program starts")
    connect_to_mongodb(host=config.SERVER_URL)   
    response = get_news_from_cryptocompare()
    articles = create_article_objects(response)
    insert_articles_to_db(articles)      


def insert_articles_to_db(articles):
    n_inserted = 0
    n_failed = 0
    logging.info("inserting to DB")
    for article in articles:
        try:
            x = Article.objects.insert(article)
            n_inserted += 1
            logging.info("Article objects:%s ", x.guid)
        except NotUniqueError as e:
            n_failed += 1
            x = e.args
    logging.info("Insertion")
    logging.info("n_inserted:%s ", n_inserted)
    logging.info("n_failed:%s ", n_failed)


def create_article_objects(response):
    logging.info("creating article objects from response")
    articles = []
    for i in response.json()["Data"]:
        article = create_article_from_dict(i)
        articles.append(article)
    logging.info("Created:%s", len(articles))
    return articles


def get_news_from_cryptocompare():
    logging.info("getting data")
    response = requests.get(CRYPTOCOMPARE_URL)
    logging.info("Article number given from API:%s", len(response.json()["Data"]))
    return response


def connect_to_mongodb(host):
    connect(host=host)


if __name__ == "__main__":
    main()
