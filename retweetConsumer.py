import time, json
from kafka import KafkaConsumer
consumer = KafkaConsumer('retweet',bootstrap_servers=['localhost:9092'])

for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    retweet = message.value.decode("utf-8")
    retweet = json.loads(retweet)
    """ print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
     """
    print(json.dumps(retweet, indent=2)) 
    time.sleep(5)