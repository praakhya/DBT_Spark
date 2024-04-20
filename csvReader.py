import os, json, time
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(bootstrap_servers='localhost:9092')

files = os.listdir("datasets")
print(files)
firstline = True
for file in files:
    with open(f"datasets/{file}", "r") as f:
        data = f.readlines()
        for line in data:
            jsonLine = json.loads(line)
            if firstline:
                print(json.dumps(jsonLine, indent=2))
                firstline = False
            user = jsonLine["user"]
            user["created_at"] = datetime.strptime(user["created_at"], "%a %b %d %H:%M:%S %z %Y").strftime("%Y-%m-%d %H:%M:%S.%f")
            producer.send("users",json.dumps(user).encode("UTF-8"))
            if ("retweeted_status" in jsonLine):
                tweet = jsonLine["retweeted_status"]
                retweet = jsonLine
                retweet["created_at"] = datetime.strptime(retweet["created_at"], "%a %b %d %H:%M:%S %z %Y").strftime("%Y-%m-%d %H:%M:%S.%f")
                tweet["created_at"] = datetime.strptime(tweet["created_at"], "%a %b %d %H:%M:%S %z %Y").strftime("%Y-%m-%d %H:%M:%S.%f")
                producer.send("tweet",json.dumps(tweet).encode("UTF-8"))
                producer.send("retweet",json.dumps(retweet).encode("UTF-8"))
            else:
                tweet = jsonLine
                tweet["created_at"] = datetime.strptime(tweet["created_at"], "%a %b %d %H:%M:%S %z %Y").strftime("%Y-%m-%d %H:%M:%S.%f")
                producer.send("tweet",json.dumps(tweet).encode("UTF-8"))
            #print(user)
            #time.sleep(5)
            #producer.send("tweet",line.encode("UTF-8"))
        