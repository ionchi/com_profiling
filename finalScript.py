from pymongo import MongoClient

import os

MONGO_HOST = 'localhost'
MONGO_PORT = 27017

DATABASE = 'twitter'
OUTPUT_FILE = 'results.csv'

if __name__ == "__main__":
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
        print("Removed: " + OUTPUT_FILE)
    with open(OUTPUT_FILE, "w") as output:
        output.write("clique_id,numero_nodi,amicizie_mancanti,simmetrie_mancanti,massimo_score_interessi,count_score_massimi \n")

    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[DATABASE]

    collectionClique = db.clique_input.find({})
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
                user_interests = db.user_infos.find({'user': user_id})[0]['info']['interests']['all']
                for interest in user_interests:
                    interest_score = db.user_infos.find({'user': user_id})[0]['info']['interests']['all'][interest]['score']
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
            user_friends = db.twitternetworks.find({'id': user_id})[0]['friends']   # indice 0 perché è solo una la lista di amici per utente

            for friend_id in clique['nodes']:
                if friend_id != user_id:
                    if friend_id not in user_friends:
                        other_user_friends = db.twitternetworks.find({'id': friend_id})[0]['friends']
                        if user_id not in other_user_friends:
                            not_friends += 1
                        else:
                            noSimm += 1

        maxScoreInClique = max(mappaApp.values())
        countMaxScores = 0
        for interest in mappaApp:
            if mappaApp[interest] == maxScoreInClique:
                countMaxScores += 1

        with open(OUTPUT_FILE, "a") as output:
            output.write(str(clique['_id']) + "," + str(len(clique['nodes'])) + ","
                         + str(not_friends) + "," + str(noSimm) + ","
                         + str(maxScoreInClique) + "," + str(countMaxScores) + "\n")
