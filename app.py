import os
import time

#Logger
from google.cloud import logging
logging_client = logging.Client()
log_name = "annotsv-log"
logger = logging_client.logger(log_name)

########################## SCRIPT TO CALL AnnotSV IF ITS INSTALLED ON SYSTEM
########################## AnnotSV IS USED FOR ACMG ANALYSIS

from mongo import get_mongo_db
client_db, db = get_mongo_db()
logger.log_text("AnnotSV MongoDB connection opened")

#Save to Mongo, made
def save(title,row):
	filt = {"title": title}
	db["annotsv"].replace_one(filt , row, upsert=True)
	logger.log_text(title + " AnnotSV value updated !")

#Compute ACMG
def compute_annotsv(batch_id,genomics_coordinates):

	#Write bed file
	filename = "/tmp/annotsv-" + batch_id + ".bed"
	f = open(filename,'w')
	for q in genomics_coordinates:
		dupdel = "DUP"
		if q["type"] == "loss":
			dupdel = "DEL"
		f.write(q["chr"] + "\t" + str(q["start"]) + "\t" + str(q["end"]) + "\t" + dupdel + "\n")
	f.close()
	
	ref = "GRCh37"
	if genomics_coordinates[0]["ref"] == "hg38":
		ref = "GRCh38"

	#Execute ClassifyCNV and convert result to regular dict
	os.system("cd ./AnnotSV/bin && ./AnnotSV -SVinputFile {} -outputFile {} -svtBEDcol 4 -genomeBuild {}".format(filename,filename,ref) )
	if os.path.isfile(filename + ".tsv"):
		data = open(filename + ".tsv","r")
		for l in data.split("\n"):
			tabs = l.split("\t")
			if len(tabs)> 108 and tabs[7] == "full":

				#Make title
				start = int(tabs[2]) - 1
				title = ref + "-chr" + tabs[1] + "-" + str(start) + "-" + tabs[3] + "-"
				var_type = "gain"
				if tabs[5] == "DEL":
					var_type = "loss"
				title += var_type

				#Retrieve criteria and score
				acmg_criteria = tabs[108].split(";")
				score = float(tabs[107])
				item = {"title": title, "acmg_criteria": acmg_criteria, "score": score}
				
				#Save to mongo
				save(item["title"], item)

		data.close()
		
##############Entrypoint for GCP
from flask import Flask
from flask import request

app = Flask(__name__)

@app.route("/batch", methods=["GET"])
def batch():
	t = time.time()
	logger.log_text("AnnotSV Batch")
	batch_id = request.args.get("batch-id")
	batch_data = db["cnvhub_batch"].find_one({'batchId':batch_id})["genomicCoordinates"]
	compute_annotsv(batch_id, batch_data)
	logger.log_text(str(round(time.time() - t,2)) + " AnnotSV CNV-Hub finished !")
	return {"text":"AnnotSV Batch OK !"}

if __name__ == "__main__":
	app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))