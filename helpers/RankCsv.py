import csv
import sys
import os
import re


f=open("UtInteressi.csv",'r')
reader=csv.reader(f)
proviamo={}
csv.field_size_limit(sys.maxsize)

OUTPUT_FILE = "InteressiUtentiRank.csv"

if (os.path.exists(OUTPUT_FILE)):
    os.remove(OUTPUT_FILE)
    print("Removed: " + OUTPUT_FILE)
dataset = open(OUTPUT_FILE, "w")
dataset.write("Utente,Interesse, Rank \n")
print("Created empty file: " + OUTPUT_FILE)


for raw in reader:
    app=[]
    b = re.split(':{[^}]+},*', raw[1])
    scores=[]
    parolone=raw[1]
    while parolone.find("score")>0:
        pos=parolone.find("score")
        if parolone[pos+8]>="0" and parolone[pos+8]<="9":
            if parolone[pos + 9] >= "0" and parolone[pos + 9] <= "9":
                scores.append(int(parolone[pos+7:pos+9]))
            else:
                scores.append(int(parolone[pos + 7:pos + 8]))
        else:
            scores.append(int(parolone[pos + 7]))
        parolone=parolone[pos+1:]

    for i in range(len(b)-1):
        if i==0:
            parolina=b[0][2:26]
        else:
            parolina=(b[i][1:25])
        dataset.write(raw[0]+","+parolina+","+str(scores[i])+"\n")

        parolina=parolina+","+str(scores[i])
        app.append(parolina)
    dataset.write("\n")
    proviamo[raw[0]]=app
dataset.close()

