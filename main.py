
# get_data_from_cryptocompare.py
from datetime import datetime
import requests

from mongoengine import connect, Document, ObjectIdField, IntField, URLField, StringField, ListField, DateTimeField
from mongoengine.errors import BulkWriteError, FieldDoesNotExist, NotUniqueError



CRYPTOCOMPARE_URL = 'https://min-api.cryptocompare.com/data/v2/news/?lang=EN&api_key=key'


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
    i=article_dict
    #todo rename key id to article id
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

#isloginti objectus is crypto compare

def main():
    connect_to_mongodb(collection_name="spurga")

    print("program starts")
    #intialising stats variables
    n_inserted = 0
    n_failed =0

    response = get_news_from_cryptocompare()

    print("creating article objects from response")
    articles = []
    for i in response.json()['Data']:
        article = create_article_from_dict(i)
        articles.append(article)
    print(len(articles), "Created")

    print("inserting to DB")
    for article in articles:
        try:
            x=Article.objects.insert(article)        
            n_inserted +=1            
            print('Article objects: ', x.guid)
        except NotUniqueError as e:
            n_failed +=1
            x=e.args
    
    print("complete")
        
    print("n_inserted: ", n_inserted)
    print("n_failed: ", n_failed)

def get_news_from_cryptocompare():
    print("getting data")    
    response = requests.get(CRYPTOCOMPARE_URL)
    print("Article number given from API:", len(response.json()['Data']))
    return response

def connect_to_mongodb(collection_name):
    connect(collection_name)

if __name__ == "__main__":
    main()


