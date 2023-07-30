
# remember ssh tuneling first
# remember set the environment variable OVERRIDE_ODBCJL_CONFIG to true
using ODBC, DataFrames


# need to set up if yaur ODBC driver manager is unixODBC
ODBC.setunixODBC()


conn = ODBC.Connection("telecom")
tower = DBInterface.execute(conn, "SELECT * FROM tower") |> DataFrame
