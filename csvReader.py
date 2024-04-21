"""
A Kafka Producer that reads data from files line by line and sends them as messages to correct topics
"""
import os, json, time
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(bootstrap_servers='localhost:9092')

files = os.listdir("datasets") #A list of all files in datasets directory
print(files)
firstline = True
for file in files: #Iterating through each file
    with open(f"datasets/{file}", "r") as f:
        data = f.readlines()
        for line in data: #Producing messages line by line
            jsonLine = json.loads(line) #inspecting the first json schema
            if firstline:
                print(json.dumps(jsonLine, indent=2))
                firstline = False
            user = jsonLine["user"] #Retrieving user data from json to send to user topic
            #Converting time format of created_at field to correct format
            user["created_at"] = datetime.strptime(user["created_at"], "%a %b %d %H:%M:%S %z %Y").strftime("%Y-%m-%d %H:%M:%S.%f")
            producer.send("users",json.dumps(user).encode("UTF-8"))
            if ("retweeted_status" in jsonLine): #Retrieving retweet data to send to retweet topic
                tweet = jsonLine["retweeted_status"]
                retweet = jsonLine
                retweet["created_at"] = datetime.strptime(retweet["created_at"], "%a %b %d %H:%M:%S %z %Y").strftime("%Y-%m-%d %H:%M:%S.%f")
                tweet["created_at"] = datetime.strptime(tweet["created_at"], "%a %b %d %H:%M:%S %z %Y").strftime("%Y-%m-%d %H:%M:%S.%f")
                producer.send("tweet",json.dumps(tweet).encode("UTF-8"))
                producer.send("retweet",json.dumps(retweet).encode("UTF-8"))
            else: #Retrieving normal tweet data to send to tweet topic
                tweet = jsonLine
                tweet["created_at"] = datetime.strptime(tweet["created_at"], "%a %b %d %H:%M:%S %z %Y").strftime("%Y-%m-%d %H:%M:%S.%f")
                producer.send("tweet",json.dumps(tweet).encode("UTF-8"))
            
        