from elasticsearch import  Elasticsearch
from elasticsearch.helpers import bulk
import json
import copy
from datetime import datetime


class Importing(object):
    result = {}
    setted = set()
    root_set = set()

    FROM_MONGO = True

    def __init__(self, host, index_name="es", doc_type="data", bulk_size=1000, auto_gen_id=False):
        self._host = host
        self._es = Elasticsearch(host)
        self._index_name = index_name
        self._doc_type = doc_type
        self._bulk_size = bulk_size
        self._auto_gen_id = auto_gen_id

    def set_vaule(self, parrent_key, value):
        if not parrent_key:
            return

        a = self.result
        _len = len(parrent_key)

        key_set = ".".join(parrent_key)
        if key_set in self.setted:
            return
        self.setted.add(key_set)

        if _len == 1 and parrent_key[0] in self.root_set:
            return
        self.root_set.add(parrent_key[0])

        for i in range(0, len(parrent_key)):
            key = parrent_key[i]
            if i == _len - 1:
                a[key] = value
            else:
                if key not in a:
                    a[key] = {}
                a = a[key]

    def convert_data(self, json_data, parrent_key):
        _parrent_key = copy.deepcopy(parrent_key)
        for key in json_data:
            new_list = copy.deepcopy(_parrent_key)
            new_list.append(key)

            if isinstance(json_data[key], dict):
                self.convert_data(json_data[key], new_list)
            if key.find("$") != 0:
                self.set_vaule(new_list, json_data[key])
                continue
            if key == "$numberLong":
                self.set_vaule(_parrent_key, int(json_data[key]))
            elif key == "$date":
                self.set_vaule(_parrent_key, json_data[key][:19])

    def convert_id(self, _id):
        return _id

    def load_data(self, file_path):

        ACTIONS = []
        count = 0
        i = 0

        for line in open(file_path, encoding="utf8"):
            data = json.loads(line)

            if self.FROM_MONGO:
                self.convert_data(data, [])
            else:
                self.result = data

            action = {
                "_index": self._index_name,
                "_type": self._doc_type,
            }
            if not self._auto_gen_id and "_id" in self.result:
                action['_id'] = self.convert_id(self.result['_id'])
            if "_id" in self.result:
                del self.result['_id']

            action["_source"] = self.result
            i += 1
            ACTIONS.append(action)

            self.result = {}
            self.setted = set()
            self.root_set = set()

            if (i == self._bulk_size):
                while(1):
                    try:
                        success, _ = bulk(self._es, ACTIONS, index = self._index_name, raise_on_error = True)
                        count += success
                        break
                    except Exception as e:
                        print(e)
                print(datetime.now(), count)
                i = 0
                ACTIONS = []

        while(1):
            try:
                success, _ = bulk(self._es, ACTIONS, index = self._index_name, raise_on_error=True)
                count += success
                break
            except Exception as e:
                print(e)

        print("insert %s lines" % count)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", '--file', help="path to file, that was exported from mongodb, each line is json format", default="mongo.json")
    parser.add_argument("-host", '--host', help="host of ES", default="localhost:9200")
    parser.add_argument("-i", '--index_name', help="index of data in ES", default="es")
    parser.add_argument("-d", '--doc_type', help="doc_type of data in ES", default="data")
    parser.add_argument("-b", '--bulk_size', help="Size of a bulk insert to ES", default=1000, type=int)
    parser.add_argument("-a", '--auto_gen_id', help="Auto gen ID. True if want auto genarate id", default=False, type=bool)

    args = parser.parse_args()

    inserter = Importing(host=args.host, index_name=args.index_name, doc_type=args.doc_type, bulk_size=args.bulk_size, auto_gen_id=args.auto_gen_id)
    inserter.load_data(args.file)