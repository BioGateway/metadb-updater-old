import urllib.request
import urllib.parse
import pymongo
import sys
from dataclasses import dataclass
from query_generators import *

print("Updater tool for downloading and caching the BioGateway metadatabase.")
print("Parameters: <port> <db-name> (Optional)<datatype>")


port = sys.argv[1]
dbName = sys.argv[2]

mbclient = pymongo.MongoClient("mongodb://localhost:27017/")
mbdb = mbclient[dbName]


@dataclass
class DataType:
    dbCollection: str
    graph: str
    constraint: str
    scores: bool


print('Loading data...')

print(mbdb.list_collection_names())

dataTypes = [
    DataType("prot", "prot", "?uri rdfs:subClassOf <http://semanticscience.org/resource/SIO_010043> .", True),
    DataType("gene", "gene", "?uri rdfs:subClassOf <http://semanticscience.org/resource/SIO_010035> .", True),
    DataType("omim", "omim", "", True),
    DataType("gobp", "go", generate_GO_namespace_constraint("biological_process"), True),
    DataType("gocc", "go", generate_GO_namespace_constraint("cellular_component"), True),
    DataType("gomf", "go", generate_GO_namespace_constraint("molecular_function"), True),
    DataType("prot2prot", "prot2prot", "", False),
    DataType("prot2onto", "prot2onto", "", False),
    DataType("tfac2gene", "tfac2gene", "", False)
]

if (len(sys.argv) > 3):
    type = sys.argv[3]
    dataTypes = list(filter(lambda x: x.dbCollection == type, dataTypes))

for dataType in dataTypes:
    print("Downloading label and description data for " + dataType.dbCollection + "...")
    query = generate_name_label_query(dataType.graph, dataType.constraint)
    data = urllib.request.urlopen(generateUrl(port, query))
    dbCol = mbdb[dataType.dbCollection]

    firstLine = True
    print("Updating data for " + dataType.dbCollection + "...")
    counter = 0
    for line in data:
        if (firstLine):
            firstLine = False
            continue
        if (counter%10000 == 0):
            print("Updating line " + str(counter) + "...")
        comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
        update = { "$set": { "prefLabel": comps[1], "lcLabel": comps[1].lower(), "definition": comps[2] } }
        response = dbCol.update_one({"_id": comps[0]}, update, upsert=True)

        counter += 1

    print("Downloading altLabel data for " + dataType.dbCollection + "...")
    query = generate_field_query(dataType.graph, "skos:altLabel", dataType.constraint)
    data = urllib.request.urlopen(generateUrl(port, query))
    dbCol = mbdb[dataType.dbCollection]

    firstLine = True
    print("Updating data for " + dataType.dbCollection + "...")
    counter = 0
    for line in data:
        if (firstLine):
            firstLine = False
            continue
        if (counter % 10000 == 0):
            print("Updating line " + str(counter) + "...")
        comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
        update = {"$set": {"altLabel": comps[1]}}
        response = dbCol.update_one({"_id": comps[0]}, update, upsert=True)

        counter += 1

    print("Downloading label and description data for " + dataType.dbCollection + "...")
    query = generate_scores_query(dataType.graph, dataType.constraint)
    data = urllib.request.urlopen(generateUrl(port, query))
    dbCol = mbdb[dataType.dbCollection]

    if (dataType.scores):
        firstLine = True
        print("Updating score data for " + dataType.dbCollection + "...")
        counter = 0
        for line in data:
            if (firstLine):
                firstLine = False
                continue
            if (counter % 10000 == 0):
                print("Updating line " + str(counter) + "...")
            comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
            fromScore = int(comps[1])
            toScore = int(comps[2])
            refScore = fromScore + toScore
            update = {"$set": {"refScore": refScore, "toScore" : toScore, "fromScore" : fromScore}}
            response = dbCol.update_one({"_id": comps[0]}, update, upsert=True)

            counter += 1

#query = generate_field_query("go", "rdf:label", constraint)
#query = generate_name_label_query("prot", "")
