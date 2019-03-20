import pyodbc
import pandas as pd

#-------------------------------------------------------------------

class SQLServer():
    def __init__(self, SERVER, DATABASE):
        self.SERVER = SERVER
        self.DATABASE = DATABASE
        self.connection = self.connect()

    def connect(self):
        print('[LOG] Connecting to SQL Server')
        print('      Server:   {}'.format(self.SERVER))
        print('      Database: {}'.format(self.DATABASE))

        return pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                "Server=" + self.SERVER + ";"
                                "Database=" + self.DATABASE + ";"
                                "Trusted_Connection=yes;")

    def queryToDF(self, query, chunksize = None):
        print('[LOG] Reading SQL Query')
        return pd.read_sql_query(query, self.connection, chunksize = chunksize)

#-------------------------------------------------------------------

