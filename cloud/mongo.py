import os
import pymongo

#Read credentials
f = open("cloud/mongo_credentials.config","r")
key = f.read().replace("\n","")
f.close()

#Get Document Db
def get_mongo_db():
	client = pymongo.MongoClient(key)
	return client, client["cnvhub"]
