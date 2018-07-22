import os
import csv
import sys

Input1="Output/elencoCliqueinComm.csv"
f=open(Input1,"r")
reader=csv.reader(f)
CliqueComm=[]
for row in reader:
    CliqueComm.append(row)
Input1="Output/SemanticoRankFiltraggio.csv"
f=open(Input1,"r")
reader=csv.reader(f)
Scartate=[]
for row in reader:
    Scartate.append(row)

#selezione del solo campo id

for i in range(len(Scartate)):
    Scartate[i]=Scartate[i][0]

for i in range(len(CliqueComm)):
    CliqueComm[i]=CliqueComm[i][0]

OUTPUT_FILE = "Output/RisultatiOutput3.csv"

if (os.path.exists(OUTPUT_FILE)):
    os.remove(OUTPUT_FILE)
    print("Removed: " + OUTPUT_FILE)
dataset = open(OUTPUT_FILE, "w")
dataset.write("Presi Output%, Falsi positivi %, Trovate, Precision, Recall\n")
print("Created empty file: " + OUTPUT_FILE)



Input1 = "Output/" \
         "OutputFinale60.csv"


f = open(Input1, "r")
reader = csv.reader(f)
Output = []
for row in reader:
    Output.append(row)
Output.pop(0)
conteggioAccorpate=0
conteggioScartate=0
conteggiosi=0
for i in Output:
    if i[1]=='si':
        conteggiosi+=1
        if i[0] in CliqueComm:
            conteggioAccorpate+=1
        if i[0] in Scartate:
            conteggioScartate+=1


PresiOutput=conteggioAccorpate/len(CliqueComm)
Falsi=conteggioScartate/conteggiosi
trovate=conteggiosi-conteggioScartate-conteggioAccorpate
precision=(conteggioAccorpate+trovate)/conteggiosi
recall=(conteggioAccorpate+trovate)/(len(CliqueComm)+trovate)
PresiOutput=PresiOutput*100
Falsi=Falsi*100
dataset.write("Topo60 e 3%,"+str(PresiOutput)+","+str(Falsi)+","+str(trovate)+","+str(precision)+","+str(recall)+"\n")
dataset.close()
