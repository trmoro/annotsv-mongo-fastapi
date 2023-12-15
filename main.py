from fastapi import FastAPI
from cloud.mongo import get_mongo_db
from pydantic import BaseModel
from typing import List

import time
import os

#Open MongoDB connection
client_db, db = get_mongo_db()
if db is not None:

	print("MongoDB connection opened")
	app = FastAPI()

	#Post CNV object
	class PostCNV(BaseModel):
		ref: str
		cnv: List[str]

	#Launch AnnotSV
	def launch_annotSV(db, cnvs: PostCNV):

		#Read CNV title (ex : hg19-chr2-1000000-2000000-gain)
		file_data = ""
		for title in cnvs.cnv:
			t = title.split("-")
			cnv_type = "DUP"
			if len(t) > 4:
				if t[4].lower() == "loss":
					cnv_type = "DEL"
				file_data += t[1] + "\t" + t[2] + "\t" + t[3] + "\t" + cnv_type + "\n"

		#Filename
		file_name = "bed/batch_{}.bed".format(str(time.time()).replace(".",""))

		#Write file
		f = open("./AnnotSV/bin/{}".format(file_name),"w")
		f.write(file_data)
		f.close()

		#Launch
		ref = "GRCh37"
		if cnvs.ref.lower() == "hg38":
			ref = "GRCh38"
		os.system("cd ./AnnotSV/bin && ./AnnotSV -SVinputFile {} -outputFile {} -svtBEDcol 4 -genomeBuild {}"
			.format(file_name,file_name,ref) )

		#Read
		f = open("./AnnotSV/bin/{}".format(file_name + ".tsv"),"r")
		data = f.read()
		nCNV = 0
		for l in data.split("\n"):
			tabs = l.split("\t")
			if len(tabs)> 108 and tabs[7] == "full":

				#Retrieve title and read full line
				cnv = cnvs.cnv[nCNV]
				acmg_criteria = tabs[108].split(";")
				score = float(tabs[107])
				if score > 1:
					score = 1
				elif score < -1:
					score = -1
				nCNV += 1
				mongo_item = {"title":cnv,"acmg_criteria":acmg_criteria,"score":score}

				#Upload result
				db["annotSV"].replace_one({"title":cnv},mongo_item,upsert=True)
		#Delete file
		f.close()
		os.remove("./AnnotSV/bin/{}".format(file_name))
		os.remove("./AnnotSV/bin/{}".format(file_name + ".tsv"))

		#Return data
		return data

	@app.post("/annotSV/")
	async def root(cnvs: PostCNV):
		return launch_annotSV(db,cnvs)
else:
	print("Cannot connect to MongoDB")
