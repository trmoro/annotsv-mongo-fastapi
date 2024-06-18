FROM python:3.8-slim

# Copy data
COPY . ./

# Copy AnnotSV
RUN apt-get install -y wget tar
RUN wget https://storage.googleapis.com/cnvhub/AnnotSV.tar.gz
RUN tar -xzvf AnnotSV.tar.gz

# Install Python3 requirements packages
RUN pip install -r requirements.txt

#Set Entrypoint for GCP
ENTRYPOINT ["python"]
CMD ["app.py"]