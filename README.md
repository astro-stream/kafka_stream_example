# kafka_stream_example

An example pipeline for pulling data from a cloud Kafka Cluster into Azure Blob Storage and then inserted into a Databricks Delta Table.

# Kafka Airflow Provider


Cloud Services used in example: 
- Confluent Kafka Cluster
- Azure Blob Store
- Azure Databricks
- Astro Runtime CLI w/ Docker
- Azure Key Vault


## Quick start

`create a kafka producer to your cloud topic`

```python 
    # producer.py 
    
import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer  
import string

kafka_username = 'myKafkaUser'
kafka_pass = 'myKafkaPass'
kafka_cluster = 'myKafkaCluster'

user_ids = list(range(1,1001))
recipient_ids = list(range(0,1001))

def generate_message() -> dict:
    random_user_id = random.choice(user_ids)

    #copy the recipients array
    recipient_ids_copy = recipient_ids.copy()

    #user can't send message to himself
    recipient_ids_copy.remove(random_user_id)
    random_recipient_id = random.choice(recipient_ids_copy)

    #generate a random message
    message = ''.join(random.choice(string.ascii_letters) for i in range(32))

    payload = {
        'user_id': random_user_id,
        'recipient_id': random_recipient_id,
        'message': message,
        'timestamp': datetime.now().strftime('%Y/%m/%dT%H:%M:%S.%f'),
        'value': random.randint(1, 100),
        }

    return payload

# messages will be seralised as JSON

def serializer(message):
    return json.dumps(message).encode('utf-8')

# Kafka Prodcuer
producer = KafkaProducer(
    bootstrap_servers=[f'{kafka_cluser}:9092'],
    sasl_plain_username=kafka_pass,
    sasl_plain_password=kafka_username,
    sasl_mechanism="PLAIN",
    security_protocol="SASL_SSL",
    value_serializer=serializer
)

# infinite loop
while True:
    # generate a message
    dummy_message = generate_message()
    # send it to our 'messages' topic
    print(f'Producing message @ {datetime.now()} | Message = {str(dummy_message)}')
    producer.send('intent', dummy_message)

    #sleep for a random number of seconds
    time_to_sleep = random.randint(1,3)
    time.sleep(time_to_sleep)
```

## Services to Configure 

**Azure Key Vault** 
With the Azure Key Vault secrets backend in the docker file you will need the Airflow Connections and Variables to be stored in the Key Vault with the following prefixes conenctons: 'airflow-connections' and variables: 'airflow-variables'. See the keybault used in this example: 
![alt text](https://github.com/astro-stream/kafka_stream_example/blob/main/images/keyVault.png)


**.env File** 
You will also need to configure an App SPN that has access to the key vault and include the APP SPN credentials in your .env file so the Airflow has access to the Key Vault Secrets backend: 
![alt text](https://github.com/astro-stream/kafka_stream_example/blob/main/images/secretsBackend.png) 

For client authentication, the DefaultAzureCredential from the Azure Python SDK is used as credential provider, which supports service principal, managed identity and user credentials.


### Setup on M1 Mac
Installing on M1 chip means a install of the librdkafka library before you can pip install confluent-kafka
```pip3 install --global-option=build_ext --global-option="-I/opt/homebrew/Cellar/librdkafka/1.9.2/include" --global-option="-L/opt/homebrew/Cellar/librdkafka/1.9.2/lib" confluent-kafka
```
