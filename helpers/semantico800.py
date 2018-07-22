from __future__ import print_function
import os
from pyspark.sql import SparkSession

INPUT_CSV = "InteressiUtentiRank.csv"

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("myApp") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/bigdata_db.dumpFinale") \
        .getOrCreate()

    df_communities = spark.read.format("com.mongodb.spark.sql").load()

    with open(INPUT_CSV) as f:
        clique_ids = f.readlines()
    # remove whitespace characters like `\n` at the end of each line
    clique_ids = [x.strip() for x in clique_ids]

    collection = df_communities.select('nodes', '_id.oid', 'com_id', 'cliques').collect()
    cliqueUtenti = {}
    CliqueInComm = {}
    for i in collection:
        if i.com_id is not None:
            cliqueUtenti[i.oid] = i.nodes
            CliqueInComm[i.oid] = len(i.cliques)

    UtenteInteressi = {}
    for i in clique_ids:
        try:
            i = i.split(",")
            utente = i[0]
            interesse = i[1]
            score = i[2]
            if utente != '' and utente != 'Utente':
                a = tuple([interesse, int(score)])
                if utente not in UtenteInteressi:
                    lista = []
                    lista.append(a)
                    UtenteInteressi[utente] = lista
                else:
                    UtenteInteressi[utente].append(a)
        except:
            pass

    OUTPUT_FILE = "Semantico800_soglia4.csv"

    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
        print("Removed: " + OUTPUT_FILE)

    dataSet = open(OUTPUT_FILE, "w")
    dataSet.write("com_id, Nodi, MassimaFreq, Conteggio, NumeroClique, intrusi\n")
    print("Created empty file: " + OUTPUT_FILE)

    InteressiFin = {}
    for cliq in cliqueUtenti:
        mappaApp = {}
        intrusi = 0
        lista = []
        for utente in cliqueUtenti[cliq]:
            try:
                for riga in UtenteInteressi[utente]:
                    if riga[0] in mappaApp:
                        if riga[1] >= 4:                        # mappaApp[riga[0]] = mappaApp[riga[1]]
                            mappaApp[riga[0]] += 1
                    else:
                        if riga[1] >= 4:                        # mappaApp[riga[0]] = mappaApp[riga[1]]
                            mappaApp[riga[0]] = 1
            except:
                lista.append(utente)
                intrusi += 1

        InteressiFin[cliq] = mappaApp
        massimo = max(mappaApp.values())
        conteggio = 0
        for i in mappaApp:
            if mappaApp[i] == massimo:
                conteggio += 1
        dataSet.write(str(cliq) + "," + str(len(cliqueUtenti[cliq])) + "," + str(massimo)+"," + str(conteggio)
                      + "," + str(CliqueInComm[cliq]) + "," + str(intrusi)+"\n")




