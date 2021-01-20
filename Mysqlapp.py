import pymysql
from mysqlconfig import dbConfig


conn = pymysql.connect(**dbConfig)
print(conn)

cur = conn.cursor()
cur.execute("show tables;")
print(cur.fetchall())
cur.close()
