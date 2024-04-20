from kafka import KafkaConsumer
import time, json
consumer = KafkaConsumer('users',bootstrap_servers=['localhost:9092'])

for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    plainTextUser = message.value.decode("utf-8").replace("\'", "\"")
    #print(plainTextUser)
    try:
        user = json.loads(plainTextUser)
    except Exception as e:
        print("Error parsing user({plainTextUser}): ",e)
    """ print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
     """
    #print(json.dumps(user, indent=2)) 
    time.sleep(5)