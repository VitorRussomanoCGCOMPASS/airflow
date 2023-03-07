
db_url = "tcp:cg-lz-core-db-sql001.public.568a9b46c7aa.database.windows.net,3342"
db_username = "vitor.ibanez@cgcompass.com"
db_password= "Changepass*23"
db_name = "Db_Brasil"

conn_string = 'DRIVER={ODBC Driver 17 for SQL Server};' \
                      'SERVER=' + db_url + \
                      ';DATABASE=' + db_name + \
                      ';UID=' + db_username + \
                      ';PWD=' + db_password + \
                      ';Authentication=ActiveDirectoryPassword'


import pyodbc
sql_conn = pyodbc.connect(conn_string)
cursor = sql_conn.cursor()
