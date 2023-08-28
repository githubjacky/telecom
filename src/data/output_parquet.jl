using ODBC, DataFrames, Distributions, Parquet


ODBC.setunixODBC()
conn = ODBC.Connection("telecom")


if !isfile("data/processed/network_sample.parquet")
    sql = """
    SELECT
        *
    FROM network
    """
    println("query telecom.network")
    network = DBInterface.execute(conn, sql) |> DataFrame

    idx = sample(1:size(network, 1), 80_000; replace=false)
    network_sample = network[idx, :]

    println("output network_sample.parquet")
    write_parquet("data/processed/network_sample.parquet", network_sample)
    println("output network.parquet")
    write_parquet("data/processed/network.parquet", network)
end


if !isfile("data/processed/user_info.parquet")
    println("query telecom.clean_user_info")
    sql = """
    SELECT
        *
    FROM clean_user_info
    """
    user_info = DBInterface.execute(conn, sql) |> DataFrame
    println("output clean_user_info.parquet")
    write_parquet("data/processed/user_info.parquet", user_info)
end


if !isfile("data/processed/clean_CDR.parquet")
    println("query telecom.clean_CDR")
    sql = """
    SELECT
        *
    FROM clean_CDR
    """
    user_info = DBInterface.execute(conn, sql) |> DataFrame
    println("output clean_CDR.parquet")
    write_parquet("data/processed/clean_CDR.parquet", user_info)
end
