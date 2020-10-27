from confluent_kafka import Consumer, OFFSET_BEGINNING


class ConsumerServer:

    BROKER_URL = "PLAINTEXT://localhost:9092"

    def __init__(self, topic_name):
        self.consumer = Consumer({"bootstrap.servers": "PLAINTEXT://localhost:9092", "group.id": "0"})
        self.consumer.subscribe([topic_name], on_assign=self.on_assign)

    def run(self):
        while True:
            messages = self.consumer.consume(10, timeout=1)
            print(f"Received {len(messages)} messages")
            for m in messages:
                if not m:
                    continue
                elif m.error():
                    print(f"Error: {m.error()}")
                else:
                    print(m.key(), m.value())

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        for partition in partitions:
            partition.offset = OFFSET_BEGINNING

        consumer.assign(partitions)


if __name__ == "__main__":
    ConsumerServer("org.san-francisco.police-department-calls").run()