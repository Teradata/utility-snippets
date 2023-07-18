# Simple Vantage connection handler using the Teradata SQL driver package
# For documentation visit https://pypi.org/project/teradatasql/
# For additional examples visit https://github.com/Teradata/python-driver/tree/master/samples

import teradatasql

# Gather connection credentials (always use environment variables, parameter files, interactive prompt, etc. to protect user credentials)
tdhost='<databaseHostNameHere>'
tdUser='<databaseUserNameHere>'
tdPassword='<databaseUserPasswordHere>'

# Create a connection to Vantage 
# This connection uses LDAP as login mechanism (logmech), remove for default (TD2) or see doc for other options
con = teradatasql.connect(None, host=tdhost, user=tdUser, password=tdPassword, logmech='LDAP')

# Test connection with a cursor
with con.cursor() as cur:
    cur.execute ("select 'Hello World'")  
    print(cur.fetchall())

# Create table and catches error message if any
with con.cursor() as cur:
    try:
        cur.execute ("create table myTable (k smallint, m varchar(100));")  
    except Exception as error:
        print('Failed creating table with error: ', error.args[0].split('\n')[0])

# Insert data from a list to a table  and read 
with con.cursor() as cur:
    try:
        cur.execute ("delete from myTable")    
        cur.execute ("insert into myTable (?, ?)", [[1, 'Hello'], [2, 'World']])    
        cur.execute ("select * from myTable order by 1")
        print(cur.fetchall())
    except:
        print('Failed reading or writing from table')

# Close connection
con.close()
