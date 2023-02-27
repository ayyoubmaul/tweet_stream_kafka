from confluent_kafka import Producer
import json
import time
import logging


def main(data, topic):
    logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    p = Producer({'bootstrap.servers':'localhost:9092'})

    def receipt(err,msg):
        if err is not None:
            print('Error: {}'.format(err))
        else:
            message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
            logger.info(message)
            print(message)

    print('Kafka Producer has been initiated...')

    p.poll(1)
    p.produce(topic, data, callback=receipt)
    p.flush()

    time.sleep(3)

if __name__ == '__main__':
    main()
