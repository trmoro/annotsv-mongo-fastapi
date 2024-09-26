import os
import pymongo

#Get Document Db
def get_mongo_db():
	client = pymongo.MongoClient(os.environ["MONGOKEY"], connect=False)
	return client, client["cnvhub"]

#Get Document Db
def get_mongo_db2():
	client = pymongo.MongoClient(os.environ["MONGOKEY2"], connect=False)
	return client, client["cnvhub"]