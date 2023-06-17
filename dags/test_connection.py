import pypyodbc as odbc
import pandas as pd

server = 'loaded.database.windows.net'
database = 'airport-to-analyze'
connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:loaded.database.windows.net,1433;Database=airport-to-analyze;Uid=domini;Pwd={Dziekanchuj!};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

conn = odbc.connect(connection_string)


cursor = conn.cursor()
print('jest ok')