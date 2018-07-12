from __future__ import print_function

from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("myApp") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/bigdata_db.iter9") \
        .getOrCreate()

    # # contiene tutta la collection dumpfinale
    df_communities = spark.read.format("com.mongodb.spark.sql").load()

    # prendo tutti i nodi presenti e l'id della community
    clique = df_communities.select('nodes', '_id.oid').collect()

    df_users = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://127.0.0.1/bigdata_db.twitter_networks").load()

    resultFile = "result.txt"
    for i in clique:
        not_friends = 0

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
        with open(resultFile, "a") as output:
            output.write("clique_id: " + str(i.oid) + " - # of not friends in clique: " + str(not_friends) + "\n")

    spark.stop()
