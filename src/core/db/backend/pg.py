import os
#import psycopg2
import pymongo
import asctime from time
import re
import shutil
import data

from config import settings
from inventory.models import *

'''
@author: anant bhardwaj
@date: Oct 3, 2013

DataHub internal APIs for postgres repo_base
'''
HOST = settings.DATABASES['default']['HOST']
PORT = 27017

if settings.DATABASES['default']['PORT'] != '':
    try:
        PORT = int(settings.DATABASES['default']['PORT'])
    except:
        pass

class MGBackend:
    def __init__(self, user, password, host=HOST, port=PORT, repo_base=None):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.repo_base = repo_base

        self.__open_connection__()

    def __open_connection__(self):
        self.connection = pymongo.MongoClient(host=self.host, port=self.port)

    def reset_connection(self, repo_base):
        self.repo_base=repo_base
        self.__open_connection__()

    def close_connection(self):
        self.connection.close()

    def create_repo(self, repo):
        try:
            Repo = self.connection[repo]
            Repo.create_collection('repo_info')
            Repo['repo_info'].insert({'owner': self.user, 'time_created': asctime()})
            Repo['repo_info'].insert({'tag':'collaborator'})
            Repo.add_user(self.user, password=self.password)
            return self.handle_return(True)
        except Exception as e:
            print e
            return self.handle_return(False)

    def list_repos(self):
        repo_list = self.connection.database_names()
        rtn = []
        for name in repo_list:
            try:
                if(self.connection[name].authenticate(self.name, password=self.password)):
                    rtn += [name]
            except Exception as e:
                pass
        return self.handle_return(rtn)

    def delete_repo(self, repo, force=False):
        repo_dir = '/user_data/%s/%s' %(self.user, repo)
        if os.path.exists(repo_dir):
            shutil.rmtree(repo_dir)

        self.connection.drop_database(repo)
        return self.handle_return(True)

    def add_collaborator(self, repo, username, privileges, auto_in_future=True):
        password = User.objects.get(username=username).password
        Repo = self.connection[repo]
        Repo.add_user(username,password=password)
        coll = Repo['repo_info'].find_one({'tag':'collaborator'})
        if coll.has_key('collaborator'):
            coll['collaborator'].append(username)
        else:
            coll['collaborator'] = [username]
        Repo['repo_info'].replace_one({'tag':'collaborator'}, coll)

    def delete_collaborator(self, repo, username):
        self.connection[repo].remove_user(username)

    def list_tables(self, repo):
        res = self.list_repos()

        all_repos = [t[0] for t in res['tuples']]
        if repo not in all_repos:
            raise LookupError('Invalid repository name: %s' %(repo))

        rtn = self.connection[repo].collection_names()
        return self.handle_return(rtn)

    def list_views(self, repo):
        res = self.list_repos()

        all_repos = [t[0] for t in res['tuples']]
        if repo not in all_repos:
            raise LookupError('Invalid repository name: %s' %(repo))

        query = ''' SELECT table_name FROM information_schema.tables
                WHERE table_schema = '%s' AND table_type = 'VIEW'
                ''' %(repo)
        return self.execute_sql(query)

    def get_schema(self, table):
        tokens = table.split('.')

        if len(tokens) < 2:
            raise NameError(
                "Invalid name: '%s'.\n"
                "HINT: use <repo-name>.<table-name> " %(table))

        query = ''' SELECT column_name, data_type
                FROM information_schem a.columns
                WHERE table_name = '%s'
                AND table_schema = '%s'
            ''' %(tokens[-1], tokens[-2])
        res = self.execute_sql(query)

        if res['row_count'] < 1:
            raise NameError("Invalid reference: '%s'.\n" %(table))

        return res

    def handle_return(self, param):
        result = {
            'status': True,
            'row_count': 0,
            'tuples': [],
            'fields': []
        }

        if type(param) == bool:
            result['status'] = param
        else if type(param) == list:
            result['tuples'] = [[t,''] for t in param]

        #
    # conn = self.connection
    # c = conn.cursor()
    # c.execute(query.strip(), params)
    #
    # try:
    #   result['tuples'] = c.fetchall()
    # except:
    #   pass
    #
    # result['status'] = True
    # result['row_count'] = c.rowcount
    # if c.description:
    #   result['fields'] = [
    #       {'name': col[0], 'type': col[1]} for col in c.description]
    #
    # tokens = query.strip().split(' ', 2)
    # c.close()
        return result

    def create_user(self, username, password, create_db):
        users = self.connection['users']
        users.create_collection(username)
        users[username].insert({'username':username, 'password':password})
        # if not create_db:
        #   return
        #
        # query = ''' CREATE DATABASE %s ''' %(username)
        # self.execute_sql(query)
        #
        # query = ''' ALTER DATABASE %s OWNER TO %s ''' %(username, username)
        # return self.execute_sql(query)
        return

    def remove_user(self, username):
        users = self.connection['users']
        users.drop_collection(username)
        return handle_return(True)

    def change_password(self, username, password):
        user = self.connection['users'][username]
        user_info = user.find_one({'username':username})
        user_info['password'] = password
        user.replace_one({'username':username}, user_info)

    def list_collaborators(self, repo_base, repo):
        return handle_return(self.connection[repo]['repo_info'].find_one({'tag':'collaborator'})['collaborator'])

  def has_base_privilege(self, login, privilege):
    query = ''' SELECT has_database_privilege('%s', '%s')
            ''' %(login, privilege)
    return self.execute_sql(query)

  def has_repo_privilege(self, login, repo, privilege):
    query = ''' SELECT has_schema_privilege('%s', '%s', '%s')
            ''' %(login, repo, privilege)
    return self.execute_sql(query)

  def has_table_privilege(self, login, table, privilege):
    query = ''' SELECT has_table_privilege('%s', '%s', '%s')
            ''' %(login, table, privilege)
    return self.execute_sql(query)

  def has_column_privilege(self, login, table, column, privilege):
    query = ''' SELECT has_column_privilege('%s', '%s', '%s')
            ''' %(login, table, column, privilege)
    return self.execute_sql(query)

  def export_table(self, table_name, file_path, file_format='CSV',
      delimiter=',', header=True):
    header_option = 'HEADER' if header else ''
    return self.execute_sql(
        ''' COPY %s TO '%s'
            WITH %s %s DELIMITER '%s';
        ''' %(table_name, file_path, file_format, header_option, delimiter))

  def export_query(self, query, file_path, file_format='CSV',
      delimiter=',', header=True):
    header_option = 'HEADER' if header else ''
    return self.execute_sql(
        ''' COPY (%s) TO '%s'
            WITH %s %s DELIMITER '%s';
        ''' %(query, file_path, file_format, header_option, delimiter))

  def import_file(self, table_name, file_path, file_format='CSV',
      delimiter=',', header=True, encoding='ISO-8859-1', quote_character='"'):
    try:
      header_option = 'HEADER' if header else ''
      if quote_character == "'":
        quote_character = "''"

      escape = ''
      if delimiter.startswith('\\'):
        escape = 'E'

      return self.execute_sql(
          ''' COPY %s FROM '%s'
              WITH %s %s DELIMITER %s'%s' ENCODING '%s' QUOTE '%s';
          ''' %(table_name, file_path, file_format,
                header_option, escape, delimiter, encoding, quote_character))
    except Exception, e:
      self.execute_sql(
          ''' DROP TABLE IF EXISTS %s;
          ''' %(table_name))
      raise ImportError(e);

      """
      Try importing using dbtruck.
      """
      # return self.import_file_w_dbtruck(table_name, file_path)

  def import_file_w_dbtruck(self, table_name, file_path):
    from dbtruck.dbtruck import import_datafiles
    from dbtruck.util import get_logger
    from dbtruck.exporters.pg import PGMethods

    dbsettings = {
      'dbname': self.repo_base,
      'hostname': self.host,
      'username': self.user,
      'password': self.password,
      'port': self.port,
    }

    create_new = True
    errfile = None

    return import_datafiles([file_path], create_new, table_name, errfile,
        PGMethods, **dbsettings)
