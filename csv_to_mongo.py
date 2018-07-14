import sys
import csv
from pymongo import MongoClient

MONGO_HOST = 'localhost'
MONGO_PORT = 27017

DATABASE = 'bd_results'
COLLECTION_NAME = 'an_top_clique_singole'

FILENAME = 'clique_singole_com_topologica.csv'
COLUMNS = ('clique_id', 'numero_nodi', 'amicizie_mancanti', 'simmetrie_mancanti')


def connect_mongo():
    try:
        client = MongoClient(MONGO_HOST, MONGO_PORT)
        db = client[DATABASE]
        return db[COLLECTION_NAME]
    except Exception as e:
        print('Got an error!')
        print(e)
        sys.exit(1)


def read_csv():
    reader = open(FILENAME, 'r')
    return csv.DictReader(reader, COLUMNS)


def save_to_mongo():
    collection = connect_mongo()
    data = read_csv()

    try:
        result = collection.insert_many(data)
        print('%d rows are saved to "%s" collection in "%s" document successfully!' % (
            len(result.inserted_ids), COLLECTION_NAME, DATABASE))
        sys.exit(1)
    except Exception as e:
        print('Got an error!')
        print(e)
        sys.exit(1)


if __name__ == '__main__':
    save_to_mongo()
