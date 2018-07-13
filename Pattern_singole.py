from __future__ import print_function

from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("myApp") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/bigdata_db.dumpFinale") \
        .getOrCreate()

    # # contiene tutta la collection dumpfinale
    df_communities = spark.read.format("com.mongodb.spark.sql").load()


    # prendo tutti i nodi presenti e l'id della community
    clique = df_communities.select('nodes', '_id.oid','com_id').collect()
    CLUtenti={}
    utenti = []
    for i in clique:
        if i.com_id==None:
            utemp=[]
            for utente in i.nodes:
                utenti.append(utente)
                utemp.append(utente)
            CLUtenti[i.oid]=utemp
    utenti=list(set(utenti))
    #ho così ottenuto una lista con tutti gli utenti e una mappa con community e lista utenti

    df_interessi=spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://127.0.0.1/bigdata_db.utenti").load()

    intrusi=[]
    UtInteressi={}
    for ut in utenti:
        prova2 = df_interessi.select('user', 'info').where(df_interessi.user == ut)
        try:
            collezione=prova2.collect()
            appoggio=collezione[0].info.interests.all
            dizioAPP=appoggio.asDict()
            listaint = []
            for key in dizioAPP:
                if dizioAPP[key]!=None:
                    listaint.append(key)
            UtInteressi[ut]=listaint
        except:
            intrusi.append(ut)
        print("fatto per utente", ut)
    #avro una mappa con per ogni utente gli interessi
    Output={}
    for clique in CLUtenti:
        count=0
        interessi=[]
        for ut in CLUtenti[clique]:
            interessi.extend(UtInteressi[ut])
        








spark.stop()