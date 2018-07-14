from __future__ import print_function
from pyspark.sql import SparkSession
from pymongo import MongoClient

import sys

MONGO_HOST = 'localhost'
MONGO_PORT = 27017

DATABASE = 'bd_results'
COLLECTION_NAME = 'an_top_clique_scartate'
INPUT_CSV = 'CliqueScartate.csv'


def connect_mongo():
    try:
        client = MongoClient(MONGO_HOST, MONGO_PORT)
        db = client[DATABASE]
        return db[COLLECTION_NAME]
    except Exception as ex:
        print('Got an error!')
        print(ex)
        sys.exit(1)


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("myApp") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/bigdata_db.clique_dataset") \
        .getOrCreate()

    collection = connect_mongo()

    df_clique = spark.read.format("com.mongodb.spark.sql").load()

    # prendo tutti i nodi presenti e l'id della community
    clique = df_clique.select('nodes', '_id.oid', 'count').collect()

    with open(INPUT_CSV) as f:
        clique_ids = f.readlines()
    # remove whitespace characters like `\n` at the end of each line
    clique_ids = [x.strip() for x in clique_ids]

    df_users = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://127.0.0.1/bigdata_db.twitter_networks").load()

    count = 0
    for i in clique:
        if i.oid in clique_ids and collection.find({'clique_id': str(i.oid)}).count() < 1:
            print("clique " + str(i.oid) + "in corso di processamento ne restano :" + str(len(clique)-count))
            count += 1
            not_friends = 0
            noSimm = 0

            for user_id in i.nodes:
                # prendere la lista degli amici dell'user attuale
                user_friends = df_users.select('id', 'friends').where(df_users.id == user_id).collect()[0].friends

                # controllo l'appartenenza dei restati utenti della clique nella lista degli amici del primo
                for j in i.nodes:
                    if j != user_id:
                        if j not in user_friends:
                            other_user_friends = df_users.select('id', 'friends').where(df_users.id == j) \
                                .collect()[0].friends
                            if user_id not in other_user_friends:
                                not_friends += 1
                            else:
                                noSimm += 1
            cliqueDoc = {
                'clique_id': str(i.oid),
                'amicizie_mancanti': str(not_friends / 2),
                'simmetrie_mancanti': str(noSimm),
                'numero_nodi': str(len(i.nodes))
            }

            try:
                result = collection.insert(cliqueDoc)
                print('one row saved to "%s" collection in "%s" document successfully!' % (
                    COLLECTION_NAME, DATABASE))
            except Exception as e:
                print('Got an error!')
                print(e)
                pass

    spark.stop()
