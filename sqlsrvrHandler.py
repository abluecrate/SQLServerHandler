import pyodbc
import pandas as pd

#-------------------------------------------------------------------

class SQLServer():
    def __init__(self, SERVER, DATABASE):
        self.SERVER = SERVER
        self.DATABASE = DATABASE
        self.connection = self.connect()

    def connect(self):
        connection = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                "Server=" + self.SERVER + ";"
                                "Database=" + self.DATABASE + ";"
                                "Trusted_Connection=yes;")
        return connection

    def queryToDF(self, query):
        df = pd.read_sql_query(query, self.connection)
        return df

#-------------------------------------------------------------------

