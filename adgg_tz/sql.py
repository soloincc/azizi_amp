from django.db import connections
from terminal_output import Terminal
from settings import DATABASES

terminal = Terminal()

class Query():

    def __init__(self, db_name):
        self.db_name = db_name

        return None

    def register_database(self):
        terminal.tprint("Registering a new database '%s'" % self.db_name, 'warn')

        new_database = {}
        new_database['id'] = self.db_name
        new_database['ENGINE'] = 'django.db.backends.postgresql_psycopg2'
        new_database['NAME'] = self.db_name
        new_database['USER'] = DATABASES['default']['USER']
        new_database['PASSWORD'] = DATABASES['default']['PASSWORD']
        new_database['HOST'] = DATABASES['default']['HOST']
        new_database['PORT'] = DATABASES['default']['PORT']

        connections.databases[self.db_name] = new_database

    def execute_query(self, query, params):
        with connection.cursor() as cursor:
            cursor.execute(query, [params])