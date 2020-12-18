from kafka import KafkaConsumer, TopicPartition
from json import loads
from os import getenv
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pymysql
import statistics

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
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        self.deps = list()
        self.wths = list()
        self.stddev = 0



        #Go back to the readme.

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
                self.deps.append(message['amt'])
            else:
                self.custBalances[message['custid']] -= message['amt']
                self.wths.append(message['amt'])
            avg_deps = 0
            avg_wths = 0
            deps_dev = 0
            wth_dev = 0
            if len(self.deps) > 0:
                avg_deps = sum(self.deps)/len(self.deps)
                if len(self.deps) > 1:
                    deps_dev = statistics.stdev(self.deps)
            if len(self.wths) > 0:
                avg_wths = sum(self.wths)/len(self.wths)
                if len(self.wths) > 1:
                    wth_dev = statistics.stdev(self.wths)
            print(self.custBalances)
            print(f'Avg deposits{avg_deps}, \n'
                  f'Avg withdrawals{avg_wths}, \n'
                  f'Standard Deviation of deposits{deps_dev}, \n'
                  f'Standard Deviation of withdrawals{wth_dev}')
            print(self.deps)

if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()