import json

from confluent_kafka import Producer

class TransactionsProducer:

    def __init__(self, kafka_brokers: list[str], topic: str):
        self.producer = Producer(
            {
                'bootstrap.servers': ','.join(kafka_brokers),
                'client.id': "transactions_producer",
             }
        )
        self.topic = topic

    def send_transactions(self, transaction: dict) -> None:
        self.producer.poll(0)

        self.producer.produce(
            topic=self.topic,
            value=json.dumps(transaction).encode("utf-8"),
            callback=_delivery_report,
        )

    def flush(self):
        self.producer.flush()


def _delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))