import time
import pymongo
from query_generators import *

def get_ref(db, collection):
    return db[collection.name]

def timestamp():
    return "[" + time.strftime("%H:%M:%S", time.localtime()) + "] "

def update_labels(dataType, context):
    startTime = time.time()
    mdb = pymongo.MongoClient("mongodb://localhost:27017/")[context.dbName]
    print(timestamp() + "Downloading label and description data for " + dataType.graph + "...")
    query = generate_name_label_query(dataType.graph, dataType.constraint)
    url = generateUrl(context.baseUrl, query, context.testingMode)
    data = urllib.request.urlopen(url)

    firstLine = True
    print(timestamp() + "Updating data for " + dataType.graph + "...")
    counter = 0
    for line in data:
        if firstLine:
            firstLine = False
            continue
        if counter % 10000 == 0:
            print(timestamp() + " " + dataType.graph + " updated labels line " + str(counter) + "...")
        comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
        for collection in dataType.dbCollections:
            if collection.prefix:
                definition = collection.prefix + comps[2]
                update = {"$set": {"prefLabel": comps[1], "lcLabel": comps[1].lower(), "definition": definition}}
            else:
                update = {"$set": {"prefLabel": comps[1], "lcLabel": comps[1].lower(), "definition": comps[2]}}
            response = get_ref(mdb, collection).update_one({"_id": comps[0]}, update, upsert=True)

        counter += 1

    print(timestamp() + "Downloading synonym data for " + dataType.graph + "...")
    query = generate_field_query(dataType.graph, "skos:altLabel", dataType.constraint)
    data = urllib.request.urlopen(generateUrl(context.baseUrl, query, context.testingMode))

    firstLine = True
    print(timestamp() + "Updating data for " + dataType.graph + "...")
    counter = 0
    for line in data:
        if firstLine:
            firstLine = False
            continue
        if counter % 10000 == 0:
            print(timestamp() + " " + dataType.graph + " updated synonym line " + str(counter) + "...")
        comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
        synonym = comps[1]
        update = {"$addToSet": {"synonyms": synonym, "lcSynonyms": synonym.lower()}}
        for dbCol in dataType.dbCollections:
            response = get_ref(mdb, dbCol).update_one({"_id": comps[0]}, update, upsert=True)

        counter += 1

    durationTime = time.time() - startTime
    print(timestamp() + "Updated " + dataType.graph + " labels in " + time.strftime("%H:%M:%S.",
                                                                                    time.gmtime(durationTime)))


def update_scores(dataType, context):
    startTime = time.time()
    mdb = pymongo.MongoClient("mongodb://localhost:27017/")[context.dbName]

    print(timestamp() + "Downloading scores for " + dataType.graph + "...")
    query = generate_scores_query(dataType.graph, dataType.constraint)
    data = urllib.request.urlopen(generateUrl(context.baseUrl, query, context.testingMode))

    firstLine = True
    print(timestamp() + "Updating score data for " + dataType.graph + "...")
    counter = 0
    for line in data:
        if firstLine:
            firstLine = False
            continue
        if counter % 10000 == 0:
            print(timestamp() + " " + dataType.graph + " updated score line " + str(counter) + "...")
        comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
        fromScore = int(comps[1])
        toScore = int(comps[2])
        refScore = fromScore + toScore
        update = {"$set": {"refScore": refScore, "toScore": toScore, "fromScore": fromScore}}
        for dbCol in dataType.dbCollections:
            response = get_ref(mdb, dbCol).update_one({"_id": comps[0]}, update, upsert=True)

        counter += 1
    durationTime = time.time() - startTime
    print(timestamp() + "Updated " + dataType.graph + " scores in " + time.strftime("%H:%M:%S.",
                                                                                    time.gmtime(durationTime)))


def update_taxon(dataType, context):
    startTime = time.time()
    mdb = pymongo.MongoClient("mongodb://localhost:27017/")[context.dbName]

    print(timestamp() + "Downloading taxa data for " + dataType.graph + "...")
    query = generate_field_query(dataType.graph, "<http://purl.obolibrary.org/obo/BFO_0000052>",
                                 dataType.constraint)
    data = urllib.request.urlopen(generateUrl(context.baseUrl, query, context.testingMode))

    firstLine = True
    print(timestamp() + "Updating taxon data for " + dataType.graph + "...")
    counter = 0
    for line in data:
        if firstLine:
            firstLine = False
            continue
        if counter % 10000 == 0:
            print(timestamp() + " " + dataType.graph + " updated taxon line " + str(counter) + "...")

        comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
        taxon = comps[1]
        update = {"$set": {"taxon": taxon}}
        for dbCol in dataType.dbCollections:
            response = get_ref(mdb, dbCol).update_one({"_id": comps[0]}, update, upsert=True)

        counter += 1
    durationTime = time.time() - startTime
    print(
        timestamp() + "Updated " + dataType.graph + " taxa in " + time.strftime("%H:%M:%S.", time.gmtime(durationTime)))


def update_instances(dataType, context):
    startTime = time.time()
    mdb = pymongo.MongoClient("mongodb://localhost:27017/")[context.dbName]

    print(timestamp() + "Downloading instance data for " + dataType.graph + "...")
    query = generate_field_query(dataType.graph, "<http://schema.org/evidenceOrigin>",
                                 dataType.constraint)
    data = urllib.request.urlopen(generateUrl(context.baseUrl, query, context.testingMode))

    firstLine = True
    print(timestamp() + "Updating instance data for " + dataType.graph + "...")
    counter = 0
    for line in data:
        if firstLine:
            firstLine = False
            continue
        if counter % 10000 == 0:
            print(timestamp() + " " + dataType.graph + " updated instance data line " + str(counter) + "...")

        comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
        instance = comps[1]
        update = {"$addToSet": {"instances": instance}}
        for dbCol in dataType.dbCollections:
            response = get_ref(mdb, dbCol).update_one({"_id": comps[0]}, update, upsert=True)

        counter += 1
    durationTime = time.time() - startTime
    print(timestamp() + "Updated " + dataType.graph + " instances in " + time.strftime("%H:%M:%S.",
                                                                                       time.gmtime(durationTime)))


def update_annotationScore(dataType, context):
    startTime = time.time()
    mdb = pymongo.MongoClient("mongodb://localhost:27017/")[context.dbName]

    print(timestamp() + "Downloading annotation scores for " + dataType.graph + "...")
    query = generate_field_query(dataType.graph, "<http://schema.org/evidenceLevel>",
                                 dataType.constraint)
    data = urllib.request.urlopen(generateUrl(context.baseUrl, query, context.testingMode))

    firstLine = True
    print(timestamp() + "Updating annotation scores for " + dataType.graph + "...")
    counter = 0
    for line in data:
        if firstLine:
            firstLine = False
            continue
        if counter % 10000 == 0:
            print(timestamp() + " " + dataType.graph + " updated annotation scores line " + str(counter) + "...")
        comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
        score = int(comps[1])
        update = {"$set": {"annotationScore": score}}
        for dbCol in dataType.dbCollections:
            response = get_ref(mdb, dbCol).update_one({"_id": comps[0]}, update, upsert=True)

        counter += 1
    durationTime = time.time() - startTime
    print(timestamp() + "Updated " + dataType.graph + " annotationScores in " + time.strftime("%H:%M:%S.", time.gmtime(
        durationTime)))
