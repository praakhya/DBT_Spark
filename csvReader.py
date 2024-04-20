import os, json, time
from kafka import KafkaProducer

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
            producer.send("users",json.dumps(user).encode("UTF-8"))
            if ("retweeted_status" in jsonLine):
                tweet = jsonLine["retweeted_status"]
                retweet = jsonLine
                producer.send("tweet",json.dumps(tweet).encode("UTF-8"))
                producer.send("retweet",json.dumps(retweet).encode("UTF-8"))
            else:
                tweet = jsonLine
                producer.send("tweet",json.dumps(tweet).encode("UTF-8"))
            #print(user)
            #time.sleep(5)
            #producer.send("tweet",line.encode("UTF-8"))
        