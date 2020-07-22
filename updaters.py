import time
from pymongo import IndexModel, ASCENDING, TEXT, DESCENDING, MongoClient
from query_generators import *
import multiprocessing as mp


def startBatches(dataType, name, target, context, query_batch_size):
    processes = []

    print(timestamp() + "Counting " + dataType.graph + " " + name + "...")
    count = target(dataType, context, justCount=True)
    print(timestamp() + "Found " + str(count) + " " + name + " in " + dataType.graph)
    batches = int(count / query_batch_size) + 1
    if batches > 0:
        print(timestamp() + "Initializing " + str(batches) + " batches.")
        for i in range(batches):
            offset = i * query_batch_size
            print(timestamp() + "Adding process: " + dataType.graph + " " + name + " " + str(i + 1) + "/" + str(
                batches) + " offset: " + str(offset))
            p = mp.Process(target=target, args=(dataType, context, offset, query_batch_size, count))
            processes.append(p)

    return processes


indexes_prot_gene = [
    IndexModel([("prefLabel", ASCENDING)]),
    IndexModel([("synonyms", ASCENDING)]),
    IndexModel([("lcSynonyms", ASCENDING)]),
    IndexModel([("definition", TEXT)]),
    IndexModel([("lcLabel", ASCENDING)]),
    IndexModel([("refScore", DESCENDING)]),
    IndexModel([("fromScore", DESCENDING)]),
    IndexModel([("toScore", DESCENDING)]),
    IndexModel([("taxon", ASCENDING)])]

indexes_goall = [
    IndexModel([("prefLabel", ASCENDING)]),
    IndexModel([("synonyms", ASCENDING)]),
    IndexModel([("lcSynonyms", ASCENDING)]),
    IndexModel([("definition", ASCENDING)]),
    IndexModel([("lcLabel", TEXT)]),
    IndexModel([("refScore", DESCENDING)]),
    IndexModel([("fromScore", DESCENDING)]),
    IndexModel([("toScore", DESCENDING)])]


def drop_and_reset_database(dbName):
    db = MongoClient("mongodb://localhost:27017/")[dbName]

    db.command("dropDatabase")
    db.prot.create_indexes(indexes_prot_gene)
    db.gene.create_indexes(indexes_prot_gene)
    db.goall.create_indexes(indexes_goall)


def get_ref(db, collection):
    return db[collection.name]


def timestamp():
    return "[" + time.strftime("%H:%M:%S", time.localtime()) + "] "


def get_count(context, query):
    count_query = generate_count_query(query)
    url = generateUrl(context.baseUrl, count_query)
    data = urllib.request.urlopen(url)
    firstLine = True
    for line in data:
        if firstLine:
            firstLine = False
            continue
        count = int(line)
        return count


def updater_worker(dataType, context, name, query, handler_function, offset=0, batchSize=0, count=0, justCount=False):
    startTime = time.time()
    if justCount:
        return get_count(context, query)

    mdb = MongoClient("mongodb://localhost:27017/")[context.dbName]
    start_message = timestamp() + "Downloading " + name + " data for " + dataType.graph
    if offset:
        start_message += " in " + str(batchSize) + " chunks. Offset: " + str(offset)
    print(start_message)
    limit = batchSize if batchSize else context.limit
    url = generateUrl(context.baseUrl, query, limit, offset)
    data = urllib.request.urlopen(url)
    durationTime = time.time() - startTime
    print(timestamp() + "Downloaded " + dataType.graph + " " + name + " data in " + time.strftime("%H:%M:%S.",
                                                                                    time.gmtime(durationTime)))
    firstLine = True
    counter = 0
    for line in data:
        if firstLine:
            firstLine = False
            continue
        if counter % 10000 == 0:
            counterWithOffset = counter + offset
            progress = str(counterWithOffset)
            if batchSize:
                progress += "/" + str(min((offset+batchSize), count))
            print(timestamp() + dataType.graph + " updated " + name + " line " + progress)
        handler_function(mdb, dataType, line)

        counter += 1

    durationTime = time.time() - startTime
    print(timestamp() + "Updated " +
          str(counter) + " " + dataType.graph + " " + name + " in " + time.strftime("%H:%M:%S.",
                                                                                    time.gmtime(durationTime)))


def update_labels(dataType, context, offset=0, batchSize=0, count=0, justCount=False):
    def update_labels_handler(mdb, dataType, line):
        comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
        for collection in dataType.dbCollections:
            if collection.prefix:
                definition = collection.prefix + comps[2]
                update = {"$set": {"prefLabel": comps[1], "lcLabel": comps[1].lower(), "definition": definition}}
            else:
                update = {"$set": {"prefLabel": comps[1], "lcLabel": comps[1].lower(), "definition": comps[2]}}
            response = get_ref(mdb, collection).update_one({"_id": comps[0]}, update, upsert=True)

    return updater_worker(dataType,
                          context, "labels",
                          generate_name_label_query(dataType.graph, dataType.constraint),
                          update_labels_handler,
                          offset,
                          batchSize,
                          count,
                          justCount)


def update_synonyms(dataType, context, offset=0, batchSize=0, count=0, justCount=False):
    def handler(mdb, dataType, line):
        comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
        synonym = comps[1]
        update = {"$addToSet": {"synonyms": synonym, "lcSynonyms": synonym.lower()}}
        for dbCol in dataType.dbCollections:
            response = get_ref(mdb, dbCol).update_one({"_id": comps[0]}, update, upsert=True)

    return updater_worker(dataType,
                          context, "synonyms",
                          generate_field_query(dataType.graph, "skos:altLabel", dataType.constraint),
                          handler,
                          offset,
                          batchSize,
                          count,
                          justCount)


def update_scores(dataType, context, offset=0, batchSize=0, count=0, justCount=False):
    def handler(mdb, dataType, line):
        comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
        fromScore = int(comps[1])
        toScore = int(comps[2])
        refScore = fromScore + toScore
        update = {"$set": {"refScore": refScore, "toScore": toScore, "fromScore": fromScore}}
        for dbCol in dataType.dbCollections:
            response = get_ref(mdb, dbCol).update_one({"_id": comps[0]}, update, upsert=True)

    return updater_worker(dataType,
                          context, "scores",
                          generate_scores_query(dataType.graph, dataType.constraint),
                          handler,
                          offset,
                          batchSize,
                          count,
                          justCount)


def update_taxon(dataType, context, offset=0, batchSize=0, count=0, justCount=False):
    def handler(mdb, dataType, line):
        comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
        taxon = comps[1]
        update = {"$set": {"taxon": taxon}}
        for dbCol in dataType.dbCollections:
            response = get_ref(mdb, dbCol).update_one({"_id": comps[0]}, update, upsert=True)

    query = generate_field_query(dataType.graph, "<http://purl.obolibrary.org/obo/BFO_0000052>",
                                 dataType.constraint)
    return updater_worker(dataType,
                          context, "taxon",
                          query,
                          handler,
                          offset,
                          batchSize,
                          count,
                          justCount)


def update_instances(dataType, context, offset=0, batchSize=0, count=0, justCount=False):
    def handler(mdb, dataType, line):
        comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
        instance = comps[1]
        update = {"$addToSet": {"instances": instance}}
        for dbCol in dataType.dbCollections:
            response = get_ref(mdb, dbCol).update_one({"_id": comps[0]}, update, upsert=True)

    query = generate_field_query(dataType.graph, "<http://schema.org/evidenceOrigin>",
                                 dataType.constraint)
    return updater_worker(dataType,
                          context, "instances",
                          query,
                          handler,
                          offset,
                          batchSize,
                          count,
                          justCount)


def update_annotationScore(dataType, context, offset=0, batchSize=0, count=0, justCount=False):
    def handler(mdb, dataType, line):
        comps = line.decode("utf-8").replace("\"", "").replace("\n", "").split("\t")
        score = int(comps[1])
        update = {"$set": {"annotationScore": score}}
        for dbCol in dataType.dbCollections:
            response = get_ref(mdb, dbCol).update_one({"_id": comps[0]}, update, upsert=True)

    query = generate_field_query(dataType.graph, "<http://schema.org/evidenceLevel>",
                                 dataType.constraint)
    return updater_worker(dataType,
                          context, "annotation scores",
                          query,
                          handler,
                          offset,
                          batchSize,
                          count,
                          justCount)
