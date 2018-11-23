#/usr/bin/python3

import humanize
import threading
from multiprocessing import Process
from multiprocessing import Manager
from multiprocessing import Value
from multiprocessing import Lock
import json
import pymongo
import time
import random
import yaml
import argparse
import socket

letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'

def monitor(lock, start_time, inserted_documents, total,size,batchSize,conf):
        percent = 0
        while percent < 100:
                duration = time.time()-start_time
                tps = inserted_documents.value / duration
                percent = (float(inserted_documents.value) / float(total)) * 100.0
                total_estimated_time = 0
                if tps != 0:
                        total_estimated_time = float(total)/float(tps)
                print("[{0:.2f}%] [{1:.2f}s / {2:.2f}s] [{3:.2f} tps] [{4} documents] ({5} bytes each)\r".format(percent,duration,total_estimated_time,tps,humanize.intcomma(inserted_documents.value),size),end="")
                time.sleep(1)
        duration = time.time()-start_time
        tps = inserted_documents.value / duration
        percent = (float(inserted_documents.value) / float(total)) * 100.0
        print("[{0:.2f}%] [{1:.2f} s] [{2:.2f} tps] [{3} documents] ({4} bytes each)                             ".format(percent,duration,tps,humanize.intcomma(inserted_documents.value),size))
        conf["duration"] = duration
        conf["tps"] = tps
        conf["client_hostname"] = socket.gethostname()
        report = open(time.strftime("report/%Y%m%d_%H%M%S_report.json"),"w")
        json.dump(conf,report)
        

def insert(lock, conf,inserted_documents):
    total = conf["documents"]
    size = conf["size"]
    batchSize = conf["batchSize"]

    #connString = "mongodb://"+conf["username"]+":"+conf["password"]+"@"+conf["host"]+":"+str(conf["port"])+"/"+conf["authSource"]
    connString = "mongodb://"+conf["host"]+":"+str(conf["port"])
    client = pymongo.MongoClient(connString)
    db = client[conf["db"]]
    col = db[conf["col"]]

    start_time = time.time()
    while True:
            with lock:
                    if not inserted_documents.value < conf["documents"]:
                            break
                    n = min(conf["batchSize"],conf["documents"]-inserted_documents.value)
                    inserted_documents.value = inserted_documents.value + n
            docs = []
            for i in range(0,n):
                    doc = {"_index":random.randint(0,total),"data":(random.choice(letters))*size}
                    docs.append(doc)
            col.insert_many(docs)

def find(lock, conf,inserted_documents):
    total = conf["documents"]
    size = conf["size"]
    batchSize = conf["batchSize"]

    #connString = "mongodb://"+conf["username"]+":"+conf["password"]+"@"+conf["host"]+":"+str(conf["port"])+"/"+conf["authSource"]
    connString = "mongodb://"+conf["host"]+":"+str(conf["port"])
    client = pymongo.MongoClient(connString)
    db = client[conf["db"]]
    col = db[conf["col"]]
    total = conf["documents"]
    
    start_time = time.time()
    while True:
            with lock:
                    if not inserted_documents.value < conf["documents"]:
                            break
                    n = min(conf["batchSize"],conf["documents"]-inserted_documents.value)
                    inserted_documents.value = inserted_documents.value + n
            #docs = []
            for i in range(0,n):
                    #doc = {"_index":random.randint(0,total),"data":(random.choice(letters))*size}
                    #docs.append(doc)
                    col.find_one({"_index":random.randint(0,total)})

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='MongoDB Insert Benchmark')
    parser.add_argument('--conf',required=False,default="conf.yml")
    args = vars(parser.parse_args())
    conf = yaml.load(open(args["conf"]))

    nthread = conf["nthread"]
    operation = conf["operation"]
    inserted_documents = Value('i',0)

    lock = Lock()
    
    Process(target=monitor, args=(lock, time.time(),inserted_documents,conf["documents"],conf["size"],conf["batchSize"],conf)).start()

    if operation == "insert":
            f = insert
    elif operation == "find":
            f = find
    
    for i in range(nthread):
        Process(target=f, args=(lock,conf,inserted_documents)).start()

