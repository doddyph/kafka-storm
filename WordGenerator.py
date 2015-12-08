import threading
import time
import random
from kafka import KafkaClient, SimpleProducer

words = ['lorem', 'ipsum', 'dolor', 'sit', 'amet']


class WordGenerator(threading.Thread):
    def __init__(self):
        super(WordGenerator, self).__init__()
        self._stop = threading.Event()
        self.kafka_client = KafkaClient('localhost:9092')
        self.kafka_producer = SimpleProducer(self.kafka_client)
        self.topic = 'words'

    def run(self):
        while True:
            if self._stop.is_set():
                return

            word = random.choice(words)
            print word
            self.kafka_producer.send_messages(self.topic, word)
            time.sleep(1)


if __name__ == '__main__':
    t = WordGenerator()
    t.start()

    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            t._stop.set()
            exit('bye...')