import json
import argparse
import random

letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'

file_registry = {}

def gendoc(schema):
    type_ = schema["type"]
    if type_ == "object":
        doc = {}
        fields = schema["fields"]
        for fieldname in fields:
            doc[fieldname] = gendoc (fields[fieldname])
        return doc
    elif type_ == "string":
        length = schema["length"]
        return (random.choice(letters))*length
    elif type_ == "integer":
        min_ = schema["min"]
        max_ = schema["max"]
        return (random.randint(min_,max_))
    elif type_ == "file":
        path = schema["path"]
        if not path in file_registry:
            file_registry[path] = open(path).read().splitlines()
        f = file_registry[path]
        return file_registry[path][random.randint(0,len(file_registry[path]))]
    elif type_ == "text":
        path = schema["path"]
        length = schema["length"]
        if not path in file_registry:
            file_registry[path] = open(path).read().splitlines()
        f = file_registry[path]
        ret = ""
        for i in range(0,length-1):
            ret += file_registry[path][random.randint(0,len(file_registry[path]))] + " " 
        ret += file_registry[path][random.randint(0,len(file_registry[path]))]
        return ret
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Document Generator')
    parser.add_argument('--schema',required=False,default="schema.json")
    args = vars(parser.parse_args())
    
    schema = json.load(open(args["schema"]))
    doc = gendoc(schema)
    print(doc)
