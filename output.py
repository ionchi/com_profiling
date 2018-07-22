from __future__ import print_function

import csv
import os

from pyspark.sql import SparkSession

media = 0.9235718909805637
deviazione = 0.08820130230264378
media_scartate = 0.7246243159329131
normal_1 = 0.1298673421199502
normal_2 = 0.02326879319967372

INPUT_CSV = "InteressiUtentiRank.csv"
INPUT2 = 'clique_topologica.csv'
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("myApp") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/bigdata_db.cliqueInput") \
        .getOrCreate()

    # # contiene tutta la collection cliqueInput
    df_communities = spark.read.format("com.mongodb.spark.sql").load()

    with open(INPUT_CSV) as f:
        clique_ids = f.readlines()
    # remove whitespace characters like `\n` at the end of each line
    clique_ids = [x.strip() for x in clique_ids]

    f2 = open(INPUT2, 'r')
    amicizie = []
    reader = csv.reader(f2)
    for row in reader:
        amicizie.append(row)
    for i in range(len(amicizie)):
        amicizie[i][1] = float(amicizie[i][1])

    # estraggo per ogni clique l'insieme degli utenti
    collezione = df_communities.select('nodes', '_id.oid').collect()
    cliqueUtenti = {}
    for i in collezione:
        cliqueUtenti[i.oid] = i.nodes

    UtenteInteressi = {}
    for i in clique_ids:
        try:
            i = i.split(",")
            utente = i[0]
            interesse = i[1]
            score = i[2]
            if (utente != '' and utente != 'Utente'):
                a = tuple([interesse, int(score)])
                if utente not in UtenteInteressi:
                    lista = []
                    lista.append(a)
                    UtenteInteressi[utente] = lista
                else:
                    UtenteInteressi[utente].append(a)
        except:
            pass

    interessiComm = {}

    OUTPUT_FILE = "OutputFinale.csv"

    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
        print("Removed: " + OUTPUT_FILE)

    dataSet = open(OUTPUT_FILE, "w")
    dataSet.write("clique_id, Community, percentuale si \n")
    print("Created empty file: " + OUTPUT_FILE)

    for cliq in cliqueUtenti:
        mappaApp = {}
        intrusi = 0
        lista = []
        for utente in cliqueUtenti[cliq]:
            try:
                for riga in UtenteInteressi[utente]:
                    if riga[0] in mappaApp:
                        if riga[1] >= 4:  # mappaApp[riga[0]] =mappaApp[riga[1]]
                            mappaApp[riga[0]] += 1
                    else:
                        if riga[1] >= 4:  # mappaApp[riga[0]] =mappaApp[riga[1]]
                            mappaApp[riga[0]] = 1
            except:
                lista.append(utente)
                intrusi += 1

        # verifica se è community o meno

        interessiComm[cliq] = mappaApp
        massimo = max(mappaApp.values())
        conteggio = 0
        appLista = {}
        for i in mappaApp:
            if mappaApp[i] == massimo:
                appLista[cliq] = i
                conteggio += 1
        freq = massimo / len(cliqueUtenti[cliq])
        if freq > media:
            semantico = 1
        else:
            if freq > (media - deviazione):
                semantico = freq
            else:
                if freq > media_scartate:
                    semantico = freq * normal_1
                else:
                    semantico = freq * normal_2
        mancanti = 0
        for cliq2 in amicizie:
            if cliq2[0] == cliq:
                mancanti = cliq2[1]
        if mancanti == 0:
            topologico = 1
        else:
            if mancanti == 1:
                topologico = 0.031
            else:
                topologico = 0
        community = semantico * 70 + topologico * 30
        if community >= 60:
            scelta = "si"
        else:
            scelta = "no"

        dataSet.write(str(cliq) + "," + str(scelta) + "," + str(community) + "\n")

    dataSet.close()
    spark.stop()
