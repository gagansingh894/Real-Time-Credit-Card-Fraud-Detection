import random
import time

import pandas as pd

from transactions_producer.producer import TransactionsProducer

# This script mocks fake transactions
def run():
    transactions_df = pd.read_csv("transactions_producer/transactions.csv")
    transactions = transactions_df.to_dict(orient="records")

    producer = TransactionsProducer(["kafka1:9092", "kafka2:9092", "kafka3:9092"], topic="transactions")

    for i, transaction in enumerate(transactions):
        if i == 0:
            # we are creating topic on first publish. This will allow for the topic to be created.
            time.sleep(10)
            producer.send_transactions(transaction)
        else:
            # mimic real traffic
            time.sleep(random.randint(1,10))
            producer.send_transactions(transaction)

    producer.flush()
