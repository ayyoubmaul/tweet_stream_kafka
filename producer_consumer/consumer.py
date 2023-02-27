from confluent_kafka import Consumer
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


def main(topic):
    c=Consumer({'bootstrap.servers':'localhost:9092','group.id':'python-consumer','auto.offset.reset':'earliest'})

    print('Available topics to consume: ', c.list_topics().topics)

    c.subscribe([topic])

    while True:
        msg=c.poll(1.0) #timeout

        print(msg)

        if msg is None:
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue

        data=msg.value().decode('utf-8')

        print(data)

        # load to postgres
        df = pd.DataFrame([{'text': data, 'recorded_at': datetime.now()}])

        df.to_csv('output_chelsea.csv', index=False, mode='a')

        # __load_to_postgres(df, 'twitter_topic')

    c.close()

def __load_to_postgres(data, table_name):
    from sqlalchemy import create_engine

    user = os.getenv('PG_USER')
    passwd = os.getenv('PG_PASSWORD')
    hostname = os.getenv('PG_HOSTNAME')
    database = os.getenv('PG_DATABASE')

    conn_string = f'postgresql://{user}:{passwd}@{hostname}:5432/{database}'

    print(conn_string)

    db = create_engine(conn_string)
    conn = db.connect()

    if data.to_sql(table_name, con=conn, if_exists='append', index=False):
        print("Table successfully loaded")

if __name__ == '__main__':
    main('twitter_topic')
