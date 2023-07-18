# Simple Vantage connection handler using the Teradata SQL driver package
# For documentation and additional examples visit https://pypi.org/project/teradataml/#documentation

# Simple Vantage 'dataframe' handler using the teradataml package

from teradataml import create_context, remove_context
import teradataml as tdml

# Gather connection credentials (always use environment variables, parameter files, interactive prompt, etc. to protect user credentials)
tdhost='<databaseHostNameHere>'
tdUser='<databaseUserNameHere>'
tdPassword='<databaseUserPasswordHere>'

# Create a Vantage context
# This connection uses LDAP as login mechanism (logmech), remove for default (TD2) or see doc for other options
create_context(host = tdhost, username = tdUser, password = tdPassword, logmech='LDAP')

# Create a teradataml dataframe pointing to table sizes in the database catalog.
df = tdml.DataFrame(tdml.in_schema('dbc','tablesizeV'))

# Sample dataframe
df.head()

# Filter in dataframe
df[(df.DataBaseName == 'dbc')].head()

# Aggregate
df.groupby('DataBaseName').sum()

# Import data in local memory as a Pandas dataframe
myPandasDf = df.head().to_pandas()

# Remove context
remove_context()