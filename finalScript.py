from pymongo import MongoClient

import os

MEDIA = 0.9235718909805637
DEVIAZIONE = 0.08820130230264378
MEDIA_SCARTATE = 0.7246243159329131
NORMAL_1 = 0.1298673421199502
NORMAL_2 = 0.02326879319967372

MONGO_HOST = 'localhost'
MONGO_PORT = 27017

DATABASE = 'bigdata_db'
OUTPUT_FILE = 'helpers/results.csv'

if __name__ == "__main__":
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
        print("Removed: " + OUTPUT_FILE)
    with open(OUTPUT_FILE, "w") as output:
        output.write("clique_id, Community, percentuale si \n")

    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[DATABASE]

    collectionClique = db.clique_dataset.find({})
    count = 0
    userInterests = {}
    for clique in collectionClique:
        mappaApp = {}
        lista = []

        intrusi = 0
        userSenzaInteressiPerClique = 0

        not_friends = 0
        noSimm = 0
        for user_id in clique['nodes']:
            # analisi semantica
            try:
                user_interests = db.user_info.find({'user': user_id})[0]['info']['interests']['all']
                for interest in user_interests:
                    interest_score = db.user_info.find({'user': user_id})[0]['info']['interests']['all'][interest]['score']
                    a = tuple([interest, int(interest_score)])
                    if user_id not in userInterests:
                        interest_tuple = [a]
                        userInterests[user_id] = interest_tuple
                    else:
                        userInterests[user_id].append(a)
                try:
                    for riga in userInterests[user_id]:
                        if riga[0] in mappaApp:
                            if riga[1] >= 4:
                                mappaApp[riga[0]] += 1
                        else:
                            if riga[1] >= 4:
                                mappaApp[riga[0]] = 1
                except:
                    lista.append(user_id)
                    intrusi += 1
            except:
                userSenzaInteressiPerClique += 1

            # analisi topologica
            user_friends = db.twitter_networks.find({'id': user_id})[0]['friends']   # indice 0 perché è solo una la lista di amici per utente

            for friend_id in clique['nodes']:
                if friend_id != user_id:
                    if friend_id not in user_friends:
                        other_user_friends = db.twitter_networks.find({'id': friend_id})[0]['friends']
                        if user_id not in other_user_friends:
                            not_friends += 1
                        else:
                            noSimm += 1

        maxScoreInClique = max(mappaApp.values())
        countMaxScores = 0
        for interest in mappaApp:
            if mappaApp[interest] == maxScoreInClique:
                countMaxScores += 1

        freq = maxScoreInClique / len(clique['nodes'])
        if freq > MEDIA:
            semantico = 1
        else:
            if freq > (MEDIA - DEVIAZIONE):
                semantico = freq
            else:
                if freq > MEDIA_SCARTATE:
                    semantico = freq * NORMAL_1
                else:
                    semantico = freq * NORMAL_2

        if not_friends == 0:
            topologico = 1
        else:
            if not_friends == 1:
                topologico = 0.031
            else:
                topologico = 0

        community = semantico * 70 + topologico * 30
        if community >= 60:
            scelta = "si"
        else:
            scelta = "no"

        print(str(clique['_id']) + "," + str(scelta) + "," + str(community))
        with open(OUTPUT_FILE, "a") as output:
            output.write(str(clique['_id']) + "," + str(scelta) + "," + str(community) + "\n")
