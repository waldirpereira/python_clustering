from pymongo import MongoClient

class MongoDbService:

    def __init__(self, configuracoes):
        myclient = MongoClient("mongodb://{}:{}".format(configuracoes['MONGODB_SERVER'], configuracoes['MONGODB_PORT']))
        mydb = myclient[configuracoes['MONGODB_DB']]
        mydb.authenticate(configuracoes['MONGODB_USER'],configuracoes['MONGODB_PASSWORD'])
        self.collection = mydb[configuracoes['MONGODB_COLLECTION']]

    def grava(self, clusters):
        self.collection.delete_many({})
        x = self.collection.insert_many(clusters)
        return x
