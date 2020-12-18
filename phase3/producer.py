from time import sleep
from json import dumps
from kafka import KafkaProducer,KafkaConsumer
import time
import random


class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                      value_serializer=lambda m: dumps(m).encode('ascii'))

    def emit(self, cust=55, type="dep"):
        data = {'custid': random.randint(50, 56),
                'type': self.depOrWth(),
                'date': int(time.time()),
                'amt': random.randint(10,101)*100,
                }
        return data

    def depOrWth(self):
        return 'dep' if (random.randint(0,2) == 0) else 'wth'

    def generateRandomXactions(self, n=1000):
        for _ in range(n):
            partition_id = random.randint(0, 3)
            data = self.emit()
            print('sent', data, partition_id)
            self.producer.send('bank-customer-new', value=data, partition=partition_id)
            sleep(1)


if __name__ == "__main__":
    p = Producer()
    p.generateRandomXactions(n=20)