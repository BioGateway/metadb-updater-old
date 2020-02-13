#!/usr/bin/env python3
import urllib.request
import urllib.parse
import time
import pymongo
import sys
from dataclasses import dataclass
from query_generators import *


def timestamp():
    return "[" + time.strftime("%H:%M:%S", time.localtime()) + "] "


if (len(sys.argv) < 3):
    print(timestamp()+" Invalid arguments.")
    print(timestamp() + " Parameters: <port> <db-name> (Optional)<datatype>")
    exit(-1)

port = sys.argv[1]
dbName = sys.argv[2]

startTime = time.time()
headerText = """
%s          -------------------           METADATABASE UPDATER          -------------------
%s                Updater tool for downloading and caching the BioGateway metadatabase.    
%s                Parameters: <port> <db-name> (Optional)<datatype>                        
%s                Connecting to endpoint on port:   %s
%s                Updating database:                %s
%s          -------------------------------------------------------------------------------
""" % (timestamp(), timestamp(), timestamp(), timestamp(), port, timestamp(), dbName, timestamp())

mbclient = pymongo.MongoClient("mongodb://localhost:27017/")
mbdb = mbclient[dbName]

print(headerText)

@dataclass
class DataType:
    dbCollection: str
    graph: str
    constraint: str
    scores: bool
    taxon: bool = False


print(timestamp()+'Loading data into ' + dbName + ' using port ' + port + '...')

dataTypes = [
    DataType("prot", "prot", "?uri rdfs:subClassOf <http://semanticscience.org/resource/SIO_010043> .", True, True),
    DataType("gene", "gene", "?uri rdfs:subClassOf <http://semanticscience.org/resource/SIO_010035> .", True, True),
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

print(timestamp()+"Updating:")
print(*dataTypes, sep="\n")
print(timestamp()+"Database collections:")
print(mbdb.list_collection_names())

for dataType in dataTypes:
    print(timestamp()+"Downloading label and description data for " + dataType.dbCollection + "...")
    query = generate_name_label_query(dataType.graph, dataType.constraint)
    data = urllib.request.urlopen(generateUrl(port, query))
    dbCol = mbdb[dataType.dbCollection]

    firstLine = True
    print(timestamp()+"Updating data for " + dataType.dbCollection + "...")
    counter = 0
    for line in data:
        if (firstLine):
            firstLine = False
            continue
        if (counter % 10000 == 0):
            print("Updating line " + str(counter) + "...")
        comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
        update = {"$set": {"prefLabel": comps[1], "lcLabel": comps[1].lower(), "definition": comps[2]}}
        response = dbCol.update_one({"_id": comps[0]}, update, upsert=True)

        counter += 1

    print(timestamp()+"Downloading altLabel data for " + dataType.dbCollection + "...")
    query = generate_field_query(dataType.graph, "skos:altLabel", dataType.constraint)
    data = urllib.request.urlopen(generateUrl(port, query))
    dbCol = mbdb[dataType.dbCollection]

    firstLine = True
    print(timestamp()+"Updating data for " + dataType.dbCollection + "...")
    counter = 0
    for line in data:
        if (firstLine):
            firstLine = False
            continue
        if (counter % 10000 == 0):
            print(timestamp()+"Updating line " + str(counter) + "...")
        comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
        update = {"$set": {"altLabel": comps[1]}}
        response = dbCol.update_one({"_id": comps[0]}, update, upsert=True)

        counter += 1

    if (dataType.scores):

        print(timestamp()+"Downloading scores for " + dataType.dbCollection + "...")
        query = generate_scores_query(dataType.graph, dataType.constraint)
        data = urllib.request.urlopen(generateUrl(port, query))
        dbCol = mbdb[dataType.dbCollection]

        firstLine = True
        print(timestamp()+"Updating score data for " + dataType.dbCollection + "...")
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
            update = {"$set": {"refScore": refScore, "toScore": toScore, "fromScore": fromScore}}
            response = dbCol.update_one({"_id": comps[0]}, update, upsert=True)

            counter += 1

    if (dataType.taxon):
        print(timestamp()+"Downloading taxa data for " + dataType.dbCollection + "...")
        query = generate_field_query(dataType.graph, "<http://purl.obolibrary.org/obo/BFO_0000052>",
                                     dataType.constraint)
        data = urllib.request.urlopen(generateUrl(port, query))
        dbCol = mbdb[dataType.dbCollection]

        firstLine = True
        print(timestamp()+"Updating taxon data for " + dataType.dbCollection + "...")
        counter = 0
        for line in data:
            if (firstLine):
                firstLine = False
                continue
            if (counter % 10000 == 0):
                print(timestamp()+"Updating line " + str(counter) + "...")
            comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
            taxon = comps[1]
            update = {"$set": {"taxon": taxon}}
            response = dbCol.update_one({"_id": comps[0]}, update, upsert=True)

            counter += 1

    durationTime = time.time() - startTime
    completionText = """
    %s          -------------------            UPDATE COMPLETE              -------------------
    %s                Update completed in %s
    %s          -------------------------------------------------------------------------------
    """ % (timestamp(), timestamp(), time.strftime("%H:%M:%S", time.gmtime(durationTime)), timestamp())
    print(completionText)