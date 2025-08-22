import pandas as pd

from transactions_producer.producer import TransactionsProducer

if __name__ == "__main__":
    transactions_df = pd.read_csv("transactions_producer/transactions.csv")
    transactions = transactions_df.to_dict(orient="records")

    producer = TransactionsProducer(["0.0.0.0:9092", "0.0.0.0:9094", "0.0.0.0:9096"], topic="transactions")

    for transaction in transactions:
        producer.send_transactions(transaction)
        break

    producer.flush()
