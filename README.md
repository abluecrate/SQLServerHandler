# SQLServerHandler
 - Small class that handles connections to Microsoft SQL Server
 - Reads SQL Queries into Pandas DataFrames
--------------------------------------------------------------
Format:

myDatabase = SQLServer('SERVER', 'DATABASE')

myQuery = 'select top (50) Names \
         from DATABASE.myTable'

data = myDatabase.queryToDF(myQuery)

--------------------------------------------------------------
Utilizes: pandas, pyodbc
