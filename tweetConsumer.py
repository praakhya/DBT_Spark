import time, json
from kafka import KafkaConsumer
consumer = KafkaConsumer('tweet',bootstrap_servers=['localhost:9092'])

for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    tweet = message.value.decode("utf-8")
    tweet = json.loads(tweet)
    """ print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
     """
    print(json.dumps(tweet, indent=2)) 
    time.sleep(5)