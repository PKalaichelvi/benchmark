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
import pymongo
import elasticsearch

letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'

file_registry = {}

def gendoc(schema,env):
    type_ = schema["type"]
    final_ret = None
    if type_ == "object":
        doc = {}
        fields = schema["fields"]
        for fieldname in fields:
            doc[fieldname] = gendoc (fields[fieldname],env)
        final_ret = doc
    elif type_ == "array":
        min_ = schema["min"]
        max_ = schema["max"]
        value = schema["value"]
        l = random.randint(min_,max_)
        tab = []
        for i in range(l):
            tab.append(gendoc(value,env))
        final_ret = tab
    elif type_ == "cststring":
        content = schema["content"]
        final_ret =  content
    elif type_ == "string":
        length = schema["length"]
        final_ret =  (random.choice(letters))*length

    elif type_ == "refprefstring":
        pref1 = env[schema["ref"]]
        pref2 = schema["prefix"]
        v = gendoc(schema["value"],env)
        final_ret = str(pref1) + pref2 + str(v)

    elif type_ == "integer":
        min_ = schema["min"]
        max_ = schema["max"]
        final_ret =  (random.randint(min_,max_))
    elif type_ == "float":
        min_ = schema["min"]
        max_ = schema["max"]
        final_ret =  min_ + random.random() * (max_ - min_)
    elif type_ == "file":
        path = schema["path"]
        if not path in file_registry:
            file_registry[path] = open(path).read().splitlines()
        f = file_registry[path]
        final_ret = file_registry[path][random.randint(0,len(file_registry[path])-1)]
    elif type_ == "text":
        path = schema["path"]
        length = schema["length"]
        if not path in file_registry:
            file_registry[path] = open(path).read().splitlines()
        f = file_registry[path]
        ret = ""
        for i in range(0,length-1):
            l = len(file_registry[path])
            j = random.randint(0,l-1)
            ret += file_registry[path][j] + " " 
        ret += file_registry[path][random.randint(0,len(file_registry[path])-1)]
        final_ret = ret
    else:
        raise Exception("type not existing " + type_)
    if "ident" in schema:
        ident = schema["ident"]
        env[ident] = final_ret
    return final_ret

def monitor(lock, start_time, inserted_documents, total,batchSize,conf):
        percent = 0
        while percent < 100:
                duration = time.time()-start_time
                tps = inserted_documents.value / duration
                percent = (float(inserted_documents.value) / float(total)) * 100.0
                total_estimated_time = 0
                if tps != 0:
                        total_estimated_time = float(total)/float(tps)
                print("[{0:.2f}%] [{1:.2f}s / {2:.2f}s] [{3:.2f} tps] [{4} documents]\r".format(percent,duration,total_estimated_time,tps,humanize.intcomma(inserted_documents.value)),end="")
                time.sleep(1)
        duration = time.time()-start_time
        tps = inserted_documents.value / duration
        percent = (float(inserted_documents.value) / float(total)) * 100.0
        print("[{0:.2f}%] [{1:.2f} s] [{2:.2f} tps] [{3} documents]                             ".format(percent,duration,tps,humanize.intcomma(inserted_documents.value)))
        conf["duration"] = duration
        conf["tps"] = tps
        conf["client_hostname"] = socket.gethostname()
        report = open(time.strftime("report/%Y%m%d_%H%M%S_report.json"),"w")
        json.dump(conf,report)
        
# f_insert: doc -> () TODO TODO TODO TODO
def insert_one(lock, conf,inserted_documents,f_insert_one):
    total = conf["operation"]["number"]
    batchSize = conf["batchSize"]
    schema = conf["operation"]["schema"]

    #connString = "mongodb://"+conf["username"]+":"+conf["password"]+"@"+conf["host"]+":"+str(conf["port"])+"/"+conf["authSource"]
    connString = "mongodb://"+conf["host"]+":"+str(conf["port"])
    client = pymongo.MongoClient(connString)
    db = client[conf["db"]]
    col = db[conf["col"]]

    start_time = time.time()
    while True:
            with lock:
                    if not inserted_documents.value < total:
                            break
                    n = min(batchSize,total-inserted_documents.value)
                    inserted_documents.value = inserted_documents.value + n
            docs = []
            for i in range(0,n):
                    #doc = {"_index":random.randint(0,total),"data":(random.choice(letters))*size}
                    doc = gendoc(schema,{})
                    docs.append(doc)
            try:
                    col.insert_many(docs)
            except pymongo.errors.BulkWriteError as bwe:
                    print(bwe.details)

def insert(lock, conf,inserted_documents):
    total = conf["operation"]["number"]
    batchSize = conf["batchSize"]
    schema = conf["operation"]["schema"]

    #connString = "mongodb://"+conf["username"]+":"+conf["password"]+"@"+conf["host"]+":"+str(conf["port"])+"/"+conf["authSource"]
    connString = "mongodb://"+conf["host"]+":"+str(conf["port"])
    client = pymongo.MongoClient(connString)
    db = client[conf["db"]]
    col = db[conf["col"]]

    start_time = time.time()
    while True:
            with lock:
                    if not inserted_documents.value < total:
                            break
                    n = min(batchSize,total-inserted_documents.value)
                    inserted_documents.value = inserted_documents.value + n
            docs = []
            for i in range(0,n):
                    #doc = {"_index":random.randint(0,total),"data":(random.choice(letters))*size}
                    doc = gendoc(schema,{})
                    docs.append(doc)
            try:
                    col.insert_many(docs)
            except pymongo.errors.BulkWriteError as bwe:
                    print(bwe.details)

def find(lock, conf,inserted_documents):
    total = conf["operation"]["number"]
    batchSize = conf["batchSize"]
    query = conf["operation"]["schema"]
    
    #connString = "mongodb://"+conf["username"]+":"+conf["password"]+"@"+conf["host"]+":"+str(conf["port"])+"/"+conf["authSource"]
    connString = "mongodb://"+conf["host"]+":"+str(conf["port"])
    client = pymongo.MongoClient(connString)
    db = client[conf["db"]]
    col = db[conf["col"]]
    
    start_time = time.time()
    while True:
            with lock:
                    if not inserted_documents.value < total:
                            break
                    n = min(conf["batchSize"],total-inserted_documents.value)
                    inserted_documents.value = inserted_documents.value + n
            #docs = []
            for i in range(0,n):
                    #doc = {"_index":random.randint(0,total),"data":(random.choice(letters))*size}
                    #docs.append(doc)
                    doc = gendoc(query,{})
                    col.find(doc).count()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='MongoDB Insert Benchmark')
    parser.add_argument('--conf',required=False,default="conf.json")
    args = vars(parser.parse_args())
    conf = json.load(open(args["conf"]))

    nthread = conf["nthread"]
    operation = conf["operation"]["type"]
    inserted_documents = Value('i',0)
    
    lock = Lock()
    
    Process(target=monitor, args=(lock, time.time(),inserted_documents,conf["operation"]["number"],conf["batchSize"],conf)).start()

    if operation == "insert":
            f = insert
    elif operation == "find":
            f = find
    
    for i in range(nthread):
        Process(target=f, args=(lock,conf,inserted_documents)).start()

