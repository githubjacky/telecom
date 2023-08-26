using ODBC, DataFrames, Distributions, Parquet


function get_network()
    ODBC.setunixODBC()
    conn = ODBC.Connection("telecom")

    sql = """
    SELECT
        *
    FROM network
    """
    network = DBInterface.execute(conn, sql) |> DataFrame

    return conn, network
end

conn, network = try
    # connect to remote database
    run(`hpc`)
    println("query telecom.network")
    get_network()
catch
    println("kill all ssh process and re-connect to hpc")
    run(`killall ssh`)
    run(`hpc`)
    println("query telecom.network")
    get_network()
end
mkpath("data/processed")

idx = sample(1:size(network, 1), 80_000; replace=false)
network_sample = network[idx, :]

println("output network_sample.parquet")
write_parquet("data/processed/network_sample.parquet", network_sample)
println("output network.parquet")
write_parquet("data/processed/network.parquet", network)


println("query telecom.clean_user_info")
sql = """
SELECT
    *
FROM clean_user_info
"""
user_info = DBInterface.execute(conn, sql) |> DataFrame
println("output clean_user_info.parquet")
write_parquet("data/processed/user_info.parquet", user_info)


println("query telecom.clean_CDR")
sql = """
SELECT
    *
FROM clean_CDR
"""
user_info = DBInterface.execute(conn, sql) |> DataFrame
println("output clean_CDR.parquet")
write_parquet("data/processed/clean_CDR.parquet", user_info)
