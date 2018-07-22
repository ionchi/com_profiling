from __future__ import print_function

import os

from pyspark.sql import SparkSession

INPUT_CSV="UtenteInteressiGood.csv"
INPUT2="elencoCliqueinComm.csv"
INPUT3="CliqueScartate.csv"
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("myApp") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/bigdata_db.cliqueInput") \
        .getOrCreate()

    # # contiene tutta la collection dumpfinale
    df_communities = spark.read.format("com.mongodb.spark.sql").load()



    with open(INPUT_CSV) as f:
        clique_ids = f.readlines()
    # remove whitespace characters like `\n` at the end of each line
    clique_ids = [x.strip() for x in clique_ids]

    with open(INPUT2) as f:
        cliqueAccorpate = f.readlines()
    # remove whitespace characters like `\n` at the end of each line
    cliqueAccorpate = [x.strip() for x in cliqueAccorpate]


    with open(INPUT3) as f:
        cliqueScartate = f.readlines()
    # remove whitespace characters like `\n` at the end of each line
    cliqueScartate = [x.strip() for x in cliqueScartate]



    #ottenimento della mappa utenti interessi
    Utente_Interessi={}
    for i in clique_ids:
        a = i.split(",")
        key = a[0]
        listone = []
        for ii in a:
            if ii != key and ii != '':
                listone.append(ii)
        Utente_Interessi[key] = listone



    #estraggo per ogni clique l'insieme degli utenti
    collezione = df_communities.select('nodes', '_id.oid').collect()
    cliqueUtenti={}
    for i in collezione:
        cliqueUtenti[i.oid]=i.nodes

    interessiComm={}
    interessiCommScartate = {}
    interessiCommAccorpate = {}

    OUTPUT_FILE_ACCORPATE = "Semantico_accorpate.csv"
    OUTPUT_FILE_ISOLATE = "Semantico_isolate.csv"
    OUTPUT_FILE_SCARTATE = "Semantico_scartate.csv"

    if os.path.exists(OUTPUT_FILE_ACCORPATE):
        os.remove(OUTPUT_FILE_ACCORPATE)
        print("Removed: " + OUTPUT_FILE_ACCORPATE)

    dataSet_ACCORPATE = open(OUTPUT_FILE_ACCORPATE, "w")
    dataSet_ACCORPATE.write("clique_id, presenze massime_valore, presenze massime_count, numero_nodi, intrusi \n")
    print("Created empty file: " + OUTPUT_FILE_ACCORPATE)

    if os.path.exists(OUTPUT_FILE_ISOLATE):
        os.remove(OUTPUT_FILE_ISOLATE)
        print("Removed: " + OUTPUT_FILE_ISOLATE)

    dataSet_ISOLATE = open(OUTPUT_FILE_ISOLATE, "w")
    dataSet_ISOLATE.write("clique_id, presenze massime_valore, presenze massime_count, numero_nodi, intrusi \n")
    print("Created empty file: " + OUTPUT_FILE_ISOLATE)

    if os.path.exists(OUTPUT_FILE_SCARTATE):
        os.remove(OUTPUT_FILE_SCARTATE)
        print("Removed: " + OUTPUT_FILE_SCARTATE)

    dataSet_SCARTATE = open(OUTPUT_FILE_SCARTATE, "w")
    dataSet_SCARTATE.write("clique_id, presenze massime_valore, presenze massime_count, numero_nodi, intrusi \n")
    print("Created empty file: " + OUTPUT_FILE_SCARTATE)



    for cliq in cliqueUtenti:
        mappaApp={}
        intrusi=0
        lista=[]
        for utente in cliqueUtenti[cliq]:
            try:
                for interesse in Utente_Interessi[utente]:
                    if interesse in mappaApp:
                        mappaApp[interesse] += 1
                    else:
                        mappaApp[interesse] = 1
            except:
                lista.append(utente)
                intrusi+=1



        if cliq in cliqueScartate:
            interessiCommScartate[cliq] = mappaApp
            massimo=max(mappaApp.values())
            conteggio=0
            for i in mappaApp:
                if mappaApp[i]==massimo:
                    conteggio+=1
            dataSet_SCARTATE.write(str(cliq)+","+str(massimo)+","+str(conteggio)+","+str(len(cliqueUtenti[cliq]))+","+str(intrusi)+"\n")


        else:
            if cliq in cliqueAccorpate:
                interessiCommAccorpate[cliq] = mappaApp
                massimo = max(mappaApp.values())
                conteggio = 0
                for i in mappaApp:
                    if mappaApp[i] == massimo:
                        conteggio += 1
                dataSet_ACCORPATE.write(str(cliq) + "," + str(massimo) + "," + str(conteggio) + "," + str(
                    len(cliqueUtenti[cliq])) + "," + str(intrusi) + "\n")
            else:
                #clique singole
                interessiComm[cliq] = mappaApp
                massimo = max(mappaApp.values())
                conteggio = 0
                for i in mappaApp:
                    if mappaApp[i] == massimo:
                        conteggio += 1
                dataSet_ISOLATE.write(str(cliq) + "," + str(massimo) + "," + str(conteggio) + "," + str(
                    len(cliqueUtenti[cliq])) + "," + str(intrusi) + "\n")





    dataSet_ISOLATE.close()
    dataSet_SCARTATE.close()
    dataSet_ACCORPATE.close()
    spark.stop()








