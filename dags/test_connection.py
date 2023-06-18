from sqlalchemy import create_engine
import pandas as pd
import pyodbc
from sqlalchemy import create_engine


server = 'loaded.database.windows.net'
database = 'airport-to-analyze'
odbc_str = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:loaded.database.windows.net,1433;Database=airport-to-analyze;Uid=domini;Pwd={Dziekanchuj!};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
connect_str = 'mssql+pyodbc:///?odbc_connect=' + odbc_str
engine = create_engine(connect_str)
conn = engine.connect()

cursor = conn.cursor()

statement = '''
SELECT *
FROM INFORMATION_SCHEMA.TABLES'''

cursor.execute(statement)

print(cursor.fetchall())