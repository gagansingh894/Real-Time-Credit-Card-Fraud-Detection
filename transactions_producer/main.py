import pandas as pd

from transactions_producer.producer import TransactionsProducer

def run():
    transactions_df = pd.read_csv("transactions_producer/transactions.csv")
    transactions = transactions_df.to_dict(orient="records")

    producer = TransactionsProducer(["0.0.0.0:9092"], topic="transactions")

    for transaction in transactions:
        producer.send_transactions(transaction)

    producer.flush()
