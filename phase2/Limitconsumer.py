from kafka import KafkaConsumer, TopicPartition
from json import loads
from os import getenv
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pymysql

Base = declarative_base()

sql_usr = getenv('mysql_usr')
sql_pwd = getenv('mysql_pwd')


class Transaction(Base):
    __tablename__ = 'transaction'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True, autoincrement=True)
    custid = Column(Integer)
    type = Column(String(250), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)


class XactionConsumer:
    def __init__(self, limit):
        self.consumer = KafkaConsumer('bank-customer-events', bootstrap_servers=['localhost:9092'],
                                      value_deserializer=lambda m: loads(m.decode('ascii')))
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        self.limit = limit

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                if (self.custBalances[message['custid']] - message['amt']) < self.limit:
                    print("Negative balance limit reached")
                else:
                    self.custBalances[message['custid']] -= message['amt']

            print(self.custBalances)


if __name__ == "__main__":
    c = XactionConsumer(-5000)
    c.handleMessages()