from kafka import KafkaConsumer,TopicPartition
from json import loads
from json import loads
from os import getenv
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pymysql

# Base = declarative_base()
#
# sql_usr = getenv('mysql_usr')
# sql_pwd = getenv('mysql_pwd')
#
#
# class Customer(Base):
#     __tablename__ = 'transaction'
#     # Here we define columns for the table person
#     # Notice that each column is also a normal Python instance attribute.
#     custid = Column(Integer, primary_key=True, autoincrement=True)
#     createdate = Column(Integer)
#     fname = Column(String(250), nullable=False)
#     lname = Column(String(250), nullable=False)


class XactionConsumer:
    def __init__(self, partition_id):
        self.consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                                      value_deserializer=lambda m: loads(m.decode('ascii')))
        partitions = self.consumer.partitions_for_topic('bank-customer-new')
        print(partitions)
        # partition = TopicPartition('bank-customer-new', partition_id)
        partition = TopicPartition('bank-customer-new', partition_id)
        print(partition)
        self.ledger = {}
        self.custBalances = {}
        self.consumer.assign([partition])

    def handleMessages(self):
        print(self.consumer.assignment())
        for msg in self.consumer:
            message = msg.value
            print('{} received'.format(message))
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)


if __name__ == "__main__":
    c = XactionConsumer(2)
    c.handleMessages()
