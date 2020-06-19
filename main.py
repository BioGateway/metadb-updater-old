#!/usr/bin/env python3
import argparse
import urllib.request
import time
import pymongo
import sys
from dataclasses import dataclass
from query_generators import *


def timestamp():
    return "[" + time.strftime("%H:%M:%S", time.localtime()) + "] "


parser = argparse.ArgumentParser(
    description='Update the BioGateway Metadata Cache with new data from the SPARQL endpoint.')
parser.add_argument('hostname', metavar='hostname', type=str,
                    help='The hostname of the BioGateway SPARQL endpoint to be loaded from.')
parser.add_argument('port', metavar='port', type=str,
                    help='The port of the BioGateway SPARQL endpoint to be loaded from.')
parser.add_argument('dbName', metavar='db-name', type=str, help='The MongoDB database to store the cached data')
parser.add_argument('--datatype', type=str, help='Limit update to this data type.')
parser.add_argument('--field', type=str, help='Limit update to this field type.')
parser.add_argument('--mode', type=str, help='Can be used to specify run modes for testing.')

args = parser.parse_args()

baseUrl = args.hostname + ":" + args.port
dbName = args.dbName

TESTING_QUERIES = (args.mode == 'testing')

startTime = time.time()
lastStartTime = startTime
headerText = """
%s          -------------------           METADATABASE UPDATER          -------------------
%s                Updater tool for downloading and caching the BioGateway metadatabase.    
%s                Parameters: <hostname:port> <db-name> (Optional)<datatype> (Optional)<fieldType>  
%s                Connecting to endpoint on:        %s
%s                Updating database:                %s
%s          -------------------------------------------------------------------------------
""" % (timestamp(), timestamp(), timestamp(), timestamp(), baseUrl, timestamp(), dbName, timestamp())

mbclient = pymongo.MongoClient("mongodb://localhost:27017/")
mbdb = mbclient[dbName]

print(headerText)


@dataclass
class DatabaseCollection:
    name: str
    prefix: str = ""

    def get_reference(self):
        return mbdb[self.name]

    reference = property(get_reference)


@dataclass
class DataType:
    graph: str
    dbCollections: [DatabaseCollection]
    constraint: str
    labels: bool
    scores: bool
    taxon: bool = False
    instances: bool = False
    annotationScores: bool = False


print(timestamp() + 'Loading data into ' + dbName + ' using port ' + baseUrl + '...')

dataTypes = [
    DataType("prot", [DatabaseCollection("prot")],
             "?uri rdfs:subClassOf <http://semanticscience.org/resource/SIO_010043> .", True, True, True, True, True),
    DataType("gene", [DatabaseCollection("gene")],
             "?uri rdfs:subClassOf <http://semanticscience.org/resource/SIO_010035> .", True, True, True, True),
    DataType("omim", [DatabaseCollection("omim")], "", True, True),
    DataType("go", [DatabaseCollection("gobp"), DatabaseCollection("goall", "Biological Process")],
             generate_GO_namespace_constraint("biological_process"), True, True),
    DataType("go", [DatabaseCollection("gocc"), DatabaseCollection("goall", "Cellular Component")],
             generate_GO_namespace_constraint("cellular_component"), True, True),
    DataType("go", [DatabaseCollection("gomf"), DatabaseCollection("goall", "Molecular Function")],
             generate_GO_namespace_constraint("molecular_function"), True, True),
    DataType("prot2prot", [DatabaseCollection("prot2prot")], "", True, False, False, True),
    DataType("prot2onto", [DatabaseCollection("prot2onto")], "", True, False),
    DataType("tfac2gene", [DatabaseCollection("tfac2gene")], "", True, False)
]

limitToDatatype = args.datatype
limitToFieldType = args.field

if limitToDatatype:
    dataTypes = list(filter(lambda x: x.graph == limitToDatatype, dataTypes))

if limitToFieldType:
    for dataType in dataTypes:
        if limitToFieldType == "label":
            dataType.labels = True
            dataType.scores = False
            dataType.taxon = False
            dataType.instances = False
        if limitToFieldType == "scores":
            dataType.labels = False
            dataType.scores = True
            dataType.taxon = False
            dataType.instances = False
        if limitToFieldType == "taxon":
            dataType.labels = False
            dataType.scores = False
            dataType.taxon = True
            dataType.instances = False
        if limitToFieldType == "instances":
            dataType.labels = False
            dataType.scores = False
            dataType.taxon = False
            dataType.instances = True
        if limitToFieldType == "annotationScores":
            dataType.labels = False
            dataType.scores = False
            dataType.taxon = False
            dataType.instances = False
            dataType.annotationScores = True

print(timestamp() + "Updating:")
print(*dataTypes, sep="\n")
print(timestamp() + "Database collections:")
print(mbdb.list_collection_names())

for dataType in dataTypes:

    if dataType.labels:
        print(timestamp() + "Downloading label and description data for " + dataType.graph + "...")
        query = generate_name_label_query(dataType.graph, dataType.constraint)
        url = generateUrl(baseUrl, query, TESTING_QUERIES)
        data = urllib.request.urlopen(url)

        firstLine = True
        print(timestamp() + "Updating data for " + dataType.graph + "...")
        counter = 0
        for line in data:
            if firstLine:
                firstLine = False
                continue
            if counter % 10000 == 0:
                print(timestamp() + "Updating line " + str(counter) + "...")
            comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
            for collection in dataType.dbCollections:
                if collection.prefix:
                    definition = collection.prefix + comps[2]
                    update = {"$set": {"prefLabel": comps[1], "lcLabel": comps[1].lower(), "definition": definition}}
                else:
                    update = {"$set": {"prefLabel": comps[1], "lcLabel": comps[1].lower(), "definition": comps[2]}}
                response = collection.reference.update_one({"_id": comps[0]}, update, upsert=True)

            counter += 1

        print(timestamp() + "Downloading synonym data for " + dataType.graph + "...")
        query = generate_field_query(dataType.graph, "skos:altLabel", dataType.constraint)
        data = urllib.request.urlopen(generateUrl(baseUrl, query, TESTING_QUERIES))

        firstLine = True
        print(timestamp() + "Updating data for " + dataType.graph + "...")
        counter = 0
        for line in data:
            if firstLine:
                firstLine = False
                continue
            if counter % 10000 == 0:
                print(timestamp() + "Updating line " + str(counter) + "...")
            comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
            synonym = comps[1]
            update = {"$addToSet": {"synonyms": synonym, "lcSynonyms": synonym.lower()}}
            for dbCol in dataType.dbCollections:
                response = dbCol.reference.update_one({"_id": comps[0]}, update, upsert=True)

            counter += 1

    if dataType.scores:

        print(timestamp() + "Downloading scores for " + dataType.graph + "...")
        query = generate_scores_query(dataType.graph, dataType.constraint)
        data = urllib.request.urlopen(generateUrl(baseUrl, query, TESTING_QUERIES))

        firstLine = True
        print(timestamp() + "Updating score data for " + dataType.graph + "...")
        counter = 0
        for line in data:
            if firstLine:
                firstLine = False
                continue
            if counter % 10000 == 0:
                print("Updating line " + str(counter) + "...")
            comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
            fromScore = int(comps[1])
            toScore = int(comps[2])
            refScore = fromScore + toScore
            update = {"$set": {"refScore": refScore, "toScore": toScore, "fromScore": fromScore}}
            for dbCol in dataType.dbCollections:
                response = dbCol.reference.update_one({"_id": comps[0]}, update, upsert=True)

            counter += 1

    if dataType.taxon:
        print(timestamp() + "Downloading taxa data for " + dataType.graph + "...")
        query = generate_field_query(dataType.graph, "<http://purl.obolibrary.org/obo/BFO_0000052>",
                                     dataType.constraint)
        data = urllib.request.urlopen(generateUrl(baseUrl, query, TESTING_QUERIES))

        firstLine = True
        print(timestamp() + "Updating taxon data for " + dataType.graph + "...")
        counter = 0
        for line in data:
            if firstLine:
                firstLine = False
                continue
            if counter % 10000 == 0:
                print(timestamp() + "Updating line " + str(counter) + "...")
            comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
            taxon = comps[1]
            update = {"$set": {"taxon": taxon}}
            for dbCol in dataType.dbCollections:
                response = dbCol.reference.update_one({"_id": comps[0]}, update, upsert=True)

            counter += 1

    if dataType.instances:
        print(timestamp() + "Downloading instance data for " + dataType.graph + "...")
        query = generate_field_query(dataType.graph, "<http://schema.org/evidenceOrigin>",
                                     dataType.constraint)
        data = urllib.request.urlopen(generateUrl(baseUrl, query, TESTING_QUERIES))

        firstLine = True
        print(timestamp() + "Updating instance data for " + dataType.graph + "...")
        counter = 0
        for line in data:
            if firstLine:
                firstLine = False
                continue
            if counter % 10000 == 0:
                print(timestamp() + "Updating line " + str(counter) + "...")
            comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
            instance = comps[1]
            update = {"$addToSet": {"instances": instance}}
            for dbCol in dataType.dbCollections:
                response = dbCol.reference.update_one({"_id": comps[0]}, update, upsert=True)

            counter += 1

    if dataType.annotationScores:
        print(timestamp() + "Downloading annotation scores for " + dataType.graph + "...")
        query = generate_field_query(dataType.graph, "<http://schema.org/evidenceLevel>",
                                     dataType.constraint)
        data = urllib.request.urlopen(generateUrl(baseUrl, query, TESTING_QUERIES))

        firstLine = True
        print(timestamp() + "Updating annotation scores for " + dataType.graph + "...")
        counter = 0
        for line in data:
            if firstLine:
                firstLine = False
                continue
            if counter % 10000 == 0:
                print(timestamp() + "Updating line " + str(counter) + "...")
            comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
            score = int(comps[1])
            update = {"$set": {"annotationScore": score}}
            for dbCol in dataType.dbCollections:
                response = dbCol.reference.update_one({"_id": comps[0]}, update, upsert=True)

            counter += 1


    durationTime = time.time() - lastStartTime
    totalDurationTime = time.time() - startTime
    lastStartTime = time.time()
    completionText = """
    %s          -------------------            UPDATE COMPLETE              -------------------
    %s                Updating %s completed in %s, %s total.
    %s          -------------------------------------------------------------------------------
    """ % (timestamp(), timestamp(), dataType.graph, time.strftime("%H:%M:%S", time.gmtime(durationTime)),
           time.strftime("%H:%M:%S", time.gmtime(totalDurationTime)), timestamp())
    print(completionText)
