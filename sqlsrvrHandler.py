import pyodbc
import pandas as pd

# -------------------------------------------------------------------

class SQLServer():
    def __init__(self, SERVER, DATABASE, DRIVER='ODBC Driver 17 for SQL Server'):

        self.SERVER = SERVER
        self.DATABASE = DATABASE

        self.DRIVER = DRIVER
        self.connection = self.connect()

    def getDrivers(self):
        return pyodbc.drivers()

    def setDriver(self):
        for index, driver in enumerate(self.getDrivers()):
            print('{}.  {}\n'.format(index, driver))
        driverSelect = input('Enter Driver Number : ')
        self.DRIVER = self.getDrivers()[int(driverSelect)]
        print('Driver Selected : {}'.format(self.DRIVER))
        print('[LOG] Reconnecting')
        self.connect()

    def connect(self):
        print('[LOG] Connecting to SQL Server')
        print('      Driver:   {}'.format(self.DRIVER))
        print('      Server:   {}'.format(self.SERVER))
        print('      Database: {}'.format(self.DATABASE))

        return pyodbc.connect("Driver={" + self.DRIVER + "};"
                              "Server=" + self.SERVER + ";"
                              "Database=" + self.DATABASE + ";"
                              "Trusted_Connection=yes;")

    def queryToDF(self, query, chunksize=None):
        print('[LOG] Reading SQL Query')
        return pd.read_sql_query(query, self.connection, chunksize=chunksize)

# -------------------------------------------------------------------
