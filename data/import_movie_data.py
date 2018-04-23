import argparse
import datetime
import random

import predictionio
import pytz

SEED = 3


def import_events(client):
    random.seed(SEED)
    user_count = 0
    item_count = 0
    event_count = 0
    print(client.get_status())
    print("Importing data...")

    with open('u.user') as user_file:
        for line in user_file.readlines():
            fields = line.strip().split('|')
            client.create_event(
                event='$set',
                entity_id=fields[0],
                entity_type='user',
                properties={
                    'age': int(fields[1]),
                    'gender': fields[2],
                    'occupation': fields[3],
                    'zipcode': fields[4]
                }
            )
            user_count += 1
    print("%s users are imported." % user_count)

    with open('u.item', encoding='ISO-8859-1') as movie_file:
        for line in movie_file.readlines():
            fields = line.strip().split('|')
            client.create_event(
                event='$set',
                entity_id=fields[0],
                entity_type='item',
                properties={
                    'title': fields[1],
                    'release_date': fields[2],
                    'action': fields[6],
                    'adventure': fields[7],
                    'animation': fields[8],
                    'child': fields[9],
                    'comedy': fields[10],
                    'crime': fields[11],
                    'documentary': fields[12],
                    'drama': fields[13],
                    'fantasy': fields[14],
                    'film_noir': fields[15],
                    'horror': fields[16],
                    'musical': fields[17],
                    'mystery': fields[18],
                    'romance': fields[19],
                    'sci_fi': fields[20],
                    'thriller': fields[21],
                    'war': fields[22],
                    'western': fields[23],
                }
            )
            item_count += 1
    print("%s items are imported." % item_count)

    with open('u.data') as rating_file:
        for line in rating_file.readlines():
            fields = line.strip().split('\t')
            client.create_event(
                event="rating",
                entity_type="user",
                entity_id=fields[0],
                target_entity_type="item",
                target_entity_id=fields[1],
                event_time=datetime.datetime.fromtimestamp(int(fields[3]), tz=pytz.utc),
                properties={
                    'rating': float(fields[2])
                }

            )
            event_count += 1

    print("%s events are imported." % event_count)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Import sample data for e-commerce recommendation engine")
    parser.add_argument('--access_key', default='invald_access_key')
    parser.add_argument('--url', default="http://localhost:7070")

    args = parser.parse_args()
    print(args)

    client = predictionio.EventClient(access_key=args.access_key,
                                      url=args.url,
                                      threads=5,
                                      qsize=500)
    import_events(client)
