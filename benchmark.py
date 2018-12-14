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
#import elasticsearch

letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'

file_registry = {}

def gensort(s):
    ret = []
    for k in s:
        if s[k] == 1:
            ret.append((k,pymongo.ASCENDING))
        elif s[k] == -1:
            ret.append((k,pymongo.DESCENDING))
    return ret

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
    elif type_ == "cstinteger":
        content = schema["content"]
        final_ret =  content
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
    elif type_ == "gauss":
        min_ = schema["min"]
        max_ = schema["max"]
        mu = schema["mu"]
        sigma = schema["sigma"]
        final_ret = random.gauss(mu,sigma)
        if final_ret < min_:
            final_ret = min_
        elif final_ret > max_:
            final_ret = max_
    elif type_ == "rectangle":
        x1 = gendoc(schema["x1"],env)
        y1 = gendoc(schema["y1"],env)
        x2 = gendoc(schema["x2"],env)
        y2 = gendoc(schema["y2"],env)
        final_ret = [[x1,y1],[x2,y1],[x2,y2],[x1,y2],[x1,y1]]
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

def monitor(lock_rem, lock_fin, lock_nret, start_time, remaining_operations, finished_operations, nreturned_operations, total, batchSize, conf):
        percent = 0
        last_remaining_operations = remaining_operations.value
        last_finished_operations = finished_operations.value
        last_time = start_time
        tps = 0
        while finished_operations.value < total:
                current_time = time.time()
                current_duration = current_time - last_time
                current_finished_operations = finished_operations.value - last_finished_operations
                tps = current_finished_operations / current_duration
                last_finished_operations = finished_operations.value
                last_time = current_time
                percent = (float(finished_operations.value) / float(total)) * 100.0
                remaining_estimated_time = 0
                if tps != 0:
                        remaining_estimated_time = float(total - finished_operations.value)/float(tps)
                print("[{0:.2f}%] [{1:.2f}s / {2:.2f}s] [{3:.2f} tps] [{4} finished] [{5} remaining] [{6} returned]                  \r".format(
                    percent,
                    time.time()-start_time,
                    remaining_estimated_time,
                    tps,
                    humanize.intcomma(finished_operations.value),
                    humanize.intcomma(remaining_operations.value),
                    humanize.intcomma(nreturned_operations.value)
                ),end="")
                time.sleep(1)
        duration = time.time()-start_time
        tps = finished_operations.value / duration
        percent = (float(finished_operations.value) / float(total)) * 100.0
        print("[{0:.2f}%] [{1:.2f} s] [{2:.2f} tps] [{3} finished] [{4} remaining] [{5} returned]                 ".format(
            percent,
            duration,
            tps,
            humanize.intcomma(finished_operations.value),
            humanize.intcomma(remaining_operations.value),
            humanize.intcomma(nreturned_operations.value)))

        conf["duration"] = duration
        conf["tps"] = tps
        conf["client_hostname"] = socket.gethostname()
        conf["finished"] = finished_operations.value
        conf["remaining"] = remaining_operations.value
        conf["nreturned"] = nreturned_operations.value
        conf["nreturned_per_operation"] = nreturned_operations.value / finished_operations.value
        conf["example"] = gendoc(conf["operation"]["schema"],{})
        report = open(time.strftime("report/%Y%m%d_%H%M%S_report.json"),"w")
        json.dump(conf,report)
        
def insert(lock_rem, lock_fin, lock_nret, conf,remaining_operations,finished_operations,nreturned_operations):
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
            with lock_rem:
                    if remaining_operations.value <= 0:
                            break
                    n = min(batchSize,remaining_operations.value)
                    remaining_operations.value = remaining_operations.value - n
            docs = []
            for i in range(0,n):
                    #doc = {"_index":random.randint(0,total),"data":(random.choice(letters))*size}
                    doc = gendoc(schema,{})
                    docs.append(doc)
            try:
                    col.insert_many(docs)
                    with lock_fin:
                        finished_operations.value = finished_operations.value + n
            except pymongo.errors.BulkWriteError as bwe:
                    print(bwe.details)

def find(lock_rem, lock_fin, lock_nret,conf,remaining_operations,finished_operations,nreturned_operations):
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
            with lock_rem:
                    if remaining_operations.value <= 0:
                            break
                    n = min(conf["batchSize"],remaining_operations.value)
                    remaining_operations.value = remaining_operations.value - n
            #docs = []
            for i in range(0,n):
                    #doc = {"_index":random.randint(0,total),"data":(random.choice(letters))*size}
                    #docs.append(doc)
                    doc = gendoc(query,{})
                    j = 0
                    proj = {}
                    if "projection" in conf["operation"]:
                        proj = gendoc(conf["operation"]["projection"],{})
                    c = col.find(doc,proj)
                    if "sort" in conf["operation"]:
                        c = c.sort(gensort(conf["operation"]["sort"]))
                    if "limit" in conf["operation"]:
                        c = c.limit(conf["operation"]["limit"])
                    for doc in c:
                        j += 1
                    with lock_nret:
                        nreturned_operations.value = nreturned_operations.value + j
                    
            with lock_fin:
                finished_operations.value = finished_operations.value + n


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='MongoDB Insert Benchmark')
    parser.add_argument('--conf',required=False,default="conf.json")
    args = vars(parser.parse_args())
    conf = json.load(open(args["conf"]))

    nthread = conf["nthread"]
    operation = conf["operation"]["type"]
    remaining_operations = Value('i',conf["operation"]["number"])
    finished_operations = Value('i',0)
    nreturned_operations = Value('i',0)
    
    lock_rem = Lock()
    lock_fin = Lock()
    lock_nret = Lock()
    
    Process(target=monitor, args=(lock_rem,
                                  lock_fin,
                                  lock_nret,
                                  time.time(),
                                  remaining_operations,
                                  finished_operations,
                                  nreturned_operations,
                                  conf["operation"]["number"],
                                  conf["batchSize"],
                                  conf)).start()

    if operation == "insert":
            f = insert
    elif operation == "find":
            f = find
    
    for i in range(nthread):
        Process(target=f, args=(lock_rem,
                                lock_fin,
                                lock_nret,
                                conf,
                                remaining_operations,
                                finished_operations,
                                nreturned_operations
        )).start()

