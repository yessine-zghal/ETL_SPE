from kafka import KafkaProducer
import json
from data import get_registered_user
import time
import pandas as pd
from fetch_data import gen_data
import logging
from tqdm import tqdm as tq, trange 
import sys
import os
from spark_data import get_from_s3

logging.basicConfig(filename='../logs/producer_trans.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',level=logging.INFO)

#serialize object into json format before transmission
def json_serializer(data):

    """
    This function takes in data and serialize the object into json format
    
    """
    return json.dumps(data).encode("utf-8")
    

def produce_transcriptions():
    """
    This function creates a kafka producer object and through it send data transcriptions to the kafka topic
    
    """
    try:
        #create a kafka producer object
        logging.info("Accessing Topic..")
        print("Accessing kafka topic")
        for i in tq(range(100),desc="Accessing Broker.."):
            producer=KafkaProducer(bootstrap_servers='localhost:9092')
        
        print("Done")

    except Exception as e:
        


        logging.info("An error has occurred")
        logging.error("The following error occurred {} ".format(e.__class__))
        print("The following error occurred {} ".format(e.__class__))
        print("Exiting the system")
        sys.exit(1)
    while 1==1:
        #generating and sending transcitpions to topic 

        logging.info("Producing Transcripts..")
        print("Producing Transcriptions")
       
        data = gen_data("s3a://grouphu-text-bucket/Clean_Amharic.txt")
        
        
        for key,text in data.items():
            
            
            print("Publishing to Topic..\n")
            print(text)
            
            producer.send("groupHu_speech",key=str.encode(key),value=str.encode(text))
            print("Done...\n")
            time.sleep(4)

if __name__ == "__main__":
    #if ran as a script we want to access the following functions
    produce_transcriptions()

