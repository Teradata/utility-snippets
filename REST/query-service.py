# Using the Vantage Query Service
# For more details on Teradata Query Service usage refer the Vantage console or the swagger UI on your Vantage system: https://<yourVantageSystemURL>/api/query/swagger-ui.html#/


import requests # Used to submit HTTP requests to the Teradata Query Service
import json
import getpass
from datetime import datetime

# Get access token and set up the request url and header.
tdServiceUrl ='https://<YourVantageSystemURL>/api/' #Change to add your Vantage system URL
headers = {'Accept': 'application/json', 'Content-Type':'application/json'}
r = requests.post(tdServiceUrl + 'user/token', headers = headers, json = {'username': input("Enter Database Username - "), 'password': getpass.getpass("Enter Database Password - ")})
token = r.json()['access_token']

# Add access token to headers
headers['Authorization'] = "Bearer "+ token

# Check for systems available at to our Query Service 
r = requests.get(tdServiceUrl + 'system/systems', headers = headers)
print(r.json())

# Issue a query to a system with the query service
# Change the post URL (<YourVantageSystemNickNameHere>) to include the system name you want to issue the query to (from the systems nicknames retrieved earlier)
sql_stmt="select * from dbc.dbcinfoV;"
payload = {"format": "object", "include_columns": "true", "log_mech": "JWT"}
payload["query"] = sql_stmt
r = requests.post(tdServiceUrl + 'query/systems/<YourVantageSystemNickNameHere>/queries/', headers = headers, json = payload)


print(r.status_code)

# Store and display the results as a Pandas dataframe
from pandas import DataFrame
df=DataFrame(r.json()['results'][0]['data'])
df