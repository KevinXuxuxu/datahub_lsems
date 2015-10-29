# use UTF-8 #
"""
    THis script is to manage experiment data,
    including record infos for new data pack,
    fetching data for experiments, etc.
"""

__author__ = "Xu Fangzhou"
__email__ = "kevin.xu.fangzhou@gmail.com"

from json import *
from pymongo import *
from pandas import *
import csv
import os
import re

class Data:
    """
        structure of a single data set to manipulate on
    """
    def __init__(self, Database, name):
        self.Database = Database
        self.db = Database.DB[name]
        self.name = name

    def record(self, _id, _dict):
        """
            update MongoDB file of id with dict.
            parameters:
                _id: string, the identical id of the file.
                _dict: dict, the dict to update.
        """
        self.db.update({'id': _id}, {'$set': _dict})

    def show(self, _id):
        """
            show dile with particular id
            parameters:
                _id: string, the identical id of the file.
            output:
                the file required as a dict.
        """
        return self.db.find_one({'id': _id})

    def show_all(self):
        """
            show all files in the collection.
            output:
                required files as a list of dicts.
        """
        rtn = []
        for i in self.db.find():
            i.pop('_id')
            rtn.append(i)
        return rtn

class DSData(Data):

    def find_parent(self):
        info = self.db.find_one({'_id': 'info'})
        if info.has_key('parent') and info['parent'] != "":
            return info['parent']
        return None

    def find_root(self):
        info = self.db.find_one({'_id': 'info'})
        if info.has_key('parent') and info['parent'] != "":
            return self.Database.get_data(info['parent']).find_root()
        return self.name

    def show_info(self):
        return DataFrame([self.db.find_one({'_id':'info'})])

    def show_data(self):
        rtn = []
        for i in self.db.find():
            if i.pop('_id') != 'info':
                rtn.append(i)
        return DataFrame(rtn)

    def diff(self, commit_id1="", commit_id2=""):
        if commit_id1 == "" and commit_id2 == "":
            commit_ids = self.show_info()['commit_ids'][0]
            commit_id1, commit_id2 = commit_ids[0], commit_ids[1]
        diffs = []
        for i in self.db.find():
            if (i.pop('_id') != 'info'):
                diffs1 = {}
                diffs2 = {}
                for k in i:
                    if k not in ['_id', 'id']:
                        if not i[k].has_key(commit_id1):
                            if i[k].has_key(commit_id2):
                                diffs1[k]=''
                                diffs2[k]=i[k][commit_id2]
                        else:
                            if not i[k].has_key(commit_id2):
                                diffs2[k] = ''
                                diffs1[k] = i[k][commit_id1]
                            elif i[k][commit_id1] != i[k][commit_id2]:
                                diffs1[k] = i[k][commit_id1]
                                diffs2[k] = i[k][commit_id2]
                        if len(diffs1) > 0:
                            diffs.append({ commit_id1: diffs1, commit_id2: diffs2, 'id': i['id']})
        return DataFrame(diffs)

class ExpData(Data):

    def show_exp_names(self):
        rtn = []
        for i in self.db.find():
            rtn.append(i['exp_name'])
        return DataFrame(rtn)

    def show_exp(self, name):
        exp = self.db.find_one({'exp_name':name})
        return DataFrame(exp['exp_records'])

    def diff(self, exp_name, commit_id1="", commit_id2="", show=[]):
        if commit_id1 == "" and commit_id2 == "":
            commit_ids = self.show_exp(exp_name)['commit_id']
            commit_id1 = commit_ids[commit_ids.size - 1]
            commit_id2 = commit_ids[commit_ids.size - 2]
        for i in self.db.find_one({'exp_name':exp_name})['exp_records']:
            if i['commit_id'] == commit_id1:
                c1 = i
            if i['commit_id'] == commit_id2:
                c2 = i
        flag = True
        if show == []:
            show = c1.keys()
            flag = False
        else:
            show.append('commit_id')
        c1r, c2r = {},{}
        for key in show:
            if key not in c1.keys() and key in c2.keys():
                c2r[key] = c2[key]
            elif key not in c2.keys() and key in c1.keys():
                c1r[key] = c1[key]
            elif c1[key] != c2[key] or flag:
                c1r[key], c2r[key] = c1[key], c2[key]
        return DataFrame([c1r,c2r])

    def diff_result(self, exp_name, commit_id1='', commit_id2=''):
        if commit_id1 == "" and commit_id2 == "":
            commit_ids = self.show_exp(exp_name)['commit_id']
            commit_id1 = commit_ids[commit_ids.size - 1]
            commit_id2 = commit_ids[commit_ids.size - 2]
        for i in self.db.find_one({'exp_name':exp_name})['exp_records']:
            if i['commit_id'] == commit_id1:
                r1 = {} if not i.has_key('result') else i['result']
            if i['commit_id'] == commit_id2:
                r2 = {} if not i.has_key('result') else i['result']
        r1['commit_id'] = i['commit_id']
        r2['commit_id'] = i['commit_id']
        for k in r1.keys():
            if r2.has_key(k) and r1[k] == r2[k]:
                r1.pop(k)
                r2.pop(k)
        return DataFrame([r1,r2])

class Database:
    """
        structure of a database
    """
    def __init__(self, db="datas", address=""):
        if address == "":
            config = json.load(open(os.environ.get("HOME") + "/sandbox/config.json"))
            address = config['mongodb_url']
        client = MongoClient(address)
        self.DB = client[db]
        self.name = db

    def import_data(self, name, description="", parent="", ignore=[], it=None, _type='', **kwargs):
        if self.DB.name != 'datas':
            print "should not import data into db other than 'datas'!"
            return
        t = re.split('\.', name)

        if it: # self defined input iterator
            try:
                coll = self.DB.create_collection(name)
                if parent != "" and parent not in self.DB.collection_names():
                    raise Exception("parent data set not in DB!")
                coll.insert({'_id': 'info',
                            'name': name,
                            'type': _type,
                            'path': '~/sandbox/data/'+name,
                            'description': description,
                            'parent': parent,
                            'commit_ids': []})
                for i in it(**kwargs):
                    if 'id' not in i.keys():
                        raise Exception("no id attribute!")
                    coll.insert(i)
            except Exception as e:
                print e
                print 'Aborting'
        elif len(t) == 2 and t[-1] in ['csv','tsv']:
            # it's csv or tsv file
            try:
                coll = self.DB.create_collection(t[0])
                if parent != "" and parent not in self.DB.collection_names():
                    raise Exception("parent data set not in DB!")
                coll.insert({'_id': 'info',
                            'name':t[0],
                            'type':t[-1],
                            'path':'~/sandbox/data/'+name,
                            'description': description,
                            'parent': parent,
                            'commit_ids': []})
                fp = open(name)
                if t[-1]=='csv': r = csv.reader(fp)
                else:
                    r = csv.reader(fp, delimiter='\t', quoting=csv.QUOTE_ALL)
                title = r.next()
                if 'id' not in title:
                    raise Exception("no id attribute!")
                for v in r:
                    if len(v) == 0: break
                    f = {}
                    for i in range(len(title)):
                        if title[i] not in ignore:
                            f[title[i]]=v[i]
                    coll.insert(f)
            except Exception as e:
                print e
                print 'Aborting...'
        elif len(t) == 2 and t[-1] == 'json':
            try:
                coll = self.DB.create_collection(t[0])
                if parent != "" and parent not in self.DB.collection_names():
                    raise Exception("parent data set not in DB!")
                coll.insert({'_id': 'info',
                            'name':t[0],
                            'type':t[-1],
                            'path':'~/sandbox/data/'+name,
                            'description': description,
                            'parent': parent,
                            'commit_ids': []})
                l = json.load(open(name))
                for d in l:
                    coll.insert(d)
            except Exception as e:
                print e
                print "Aborting..."

    def generate_data(self, name, description="", parent="", ignore=[]):
        if name not in os.listdir('.'):
            print "no such data set to register: "+name
            return
        os.system("cp %s ~/sandbox/data/" %name)
        self.import_data(name, description, parent, ignore)

    def join(self, name, name_list=[], key='_id'):
        rtn = self.get_data(name).show_all()
        for i in range(0,len(rtn)):
            try:
                value = rtn[i][key]
                for n in name_list:
                    b = self.get_data(n).db.find_one({key:value})
                    if b:
                        for p in b.items():
                            if not rtn[i].has_key(p[0]):
                                rtn[i][p[0]] = p[1]
            except Exception as e:
                print e
        return rtn

    def get_data(self, name):
        if self.name == 'datas':
            return DSData(self, name)
        elif self.name == 'users':
            return ExpData(self, name)

class View:
    """
        providing joined view for datasets in database.
    """
    def __init__(self, database, name, name_list=[], key="_id"):
        self.database = database
        self.prim_ds = self.database.get_data(name)
        self.name = name
        self.name_list = name_list
        self.key = key

    def get(self, pair):
        rtn = self.prim_ds.db.find_one(pair)
        for name in self.name_list:
            b = self.database.get_data(name).db.find_one({self.key:rtn[self.key]})
            if b:
                for p in b.items():
                    if not rtn.has_key(p[0]):
                        rtn[p[0]] = p[1]
        return rtn

    def dump(self):
        return self.database.join(self.name, self.name_list, self.key)

    def dump_df(self):
        return DataFrame(self.dump()[1:])
