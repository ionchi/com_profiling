from __future__ import print_function

from pyspark.sql import SparkSession
import os


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("myApp") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/bigdata_db.dumpFinale") \
        .getOrCreate()

    OUTPUT_FILE = "AnalisiTopologica.csv"

    if (os.path.exists(OUTPUT_FILE)):
        os.remove(OUTPUT_FILE)
        print("Removed: " + OUTPUT_FILE)
    dataset = open(OUTPUT_FILE, "w")
    dataset.write("clique_id,Amicizie mancanti, Simmetrie mancanti \n")
    print("Created empty file: " + OUTPUT_FILE)
    # # contiene tutta la collection dumpfinale
    df_communities = spark.read.format("com.mongodb.spark.sql").load()

    # prendo tutti i nodi presenti e l'id della community
    clique = df_communities.select('nodes', '_id.oid','com_id').collect()

    df_users = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://127.0.0.1/bigdata_db.networktwitter").load()

    resultFile = "result.txt"
    count=0
    for i in clique:

        print("clique " +str(i.oid) + "in corso di processamento ne restano :" + str(len(clique)-count))
        count+=1
        not_friends = 0
        noSimm = 0
        if i.com_id==None:

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
                                noSimm+=1
            dataset.write(str(i.oid)+","+str(not_friends/2)+","+str(noSimm)+"\n")
            with open(resultFile, "a") as output:
                output.write("clique_id: " + str(i.oid) + " - # amicizie mancanti " + str(not_friends) + "  Simmetrie mancanti : "+str(noSimm)+"\n")
    dataset.close()
spark.stop()