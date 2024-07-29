import cudf


def main(month: str) -> None:
    """
    There are 3 steps to clean the CDRs
    1. filter outliers
    2. generate additional columns
    3. adjust the type of `CALLING_AREA_CODE` and `CALLED_AREA_CODE` to numeric
    4. rename serveral columns
    """

    # intentionally omit the `SERV_ID`, `BILLING_DURATION`, `CALLS`, `ETL_TYPE_ID`
    # to reduce memory usage
    # omit `SERV_ID` since I plan to use `ACC_NBR` as the primary key
    # omit `BILLING_DURATION` and `CALLS` as there is only one unique value
    # omit `ETL_TYPE_ID` as there isn't much vairance after filtering outliers
    dtype = {
        "ACC_NBR": "string",
        "TRAFFIC": "float32",
        "MONTH_NO": "int8",
        "CHARGE": "float32",
        "DURATION": "int32",
        "ACCT_ITEM_TYPE_CODE": "int32",
        "START_TIME": "str",
        "END_TIME": "str",
        "CALLED_AREA_CODE": "str",
        "CALLED_NBR": "str",
        "LAC": "str",
        "CELL_ID": "int8",
        "RECV_TRAFFIC": "float32",
        "CALLING_AREA_CODE": "str",
        "CALLING_NBR": "str",
        "SEND_TRAFFIC": "float32",
    }
    df = cudf.read_csv(f"data/cdr/{month}.csv", dtype=dtype)

    # 1. filter outliers
    # rule 1: START_TIME and END_TIME mush math the pattern
    # rule 2: remove calls not made from mobile phones
    time_pattern = "^(2013|2014)/(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01]) (0?[0-9]|1[0-9]|2[0-3]):.*"

    df = df[
        (~df.CALLED_NBR.str.match("^NotMobile.*"))
        & (~df.CALLING_NBR.str.match("^NotMobile.*"))
        & (df.START_TIME.str.match(time_pattern))
        & (df.END_TIME.str.match(time_pattern))
    ]

    # 2. generate additional columns
    for length in [16, 17, 18]:
        mask = df.START_TIME.str.len() == length
        df.loc[mask, "START_TIME"] = cudf.to_datetime(df.loc[mask, "START_TIME"])

    df.START_TIME = df.START_TIME.astype("datetime64[s]")
    df.time = df.START_TIME.dt.hour
    df = df.assign(time=df.START_TIME.dt.hour)
    df = df.assign(day_of_week=df.START_TIME.dt.dayofweek)

    # 3. adjust the type of `CALLING_AREA_CODE` and `CALLED_AREA_CODE` to numeric
    for col in ["CALLING_AREA_CODE", "CALLED_AREA_CODE"]:
        df[col] = cudf.to_numeric(df[col], errors="coerce", downcast="integer")
    df.dropna(inplace=True)

    # 4. rename several columns
    columns = {
        "ACC_NBR": "client_nbr",
        "MONTH_NO": "month",
        "CALLING_NBR": "calling_nbr",
        "CALLING_AREA_CODE": "calling_area_code",
        "CALLED_AREA_CODE": "called_area_code",
        "CALLED_NBR": "called_nbr",
        "TRAFFIC": "traffic",
        "CHARGE": "charge",
        "DURATION": "duration",
        "RECV_TRAFFIC": "recv_traffic",
        "SEND_TRAFFIC": "send_traffic",
        "ACCT_ITEM_TYPE_CODE": "acct_item_type_code",
        "LAC": "lac",
        "CELL_ID": "cell_id",
    }
    df.rename(columns=columns, inplace=True)

    return df[list(columns.values()) + ["time", "day_of_week"]]


if __name__ == "__main__":
    # cluster = LocalCUDACluster(
    #     CUDA_VISIBLE_DEVICES="0,1",
    #     protocol="ucx",
    #     enable_tcp_over_ucx=True,
    #     enable_infiniband=True,
    #     rmm_managed_memory=True,
    #     rmm_pool_size="24GB",
    # )
    # client = Client(cluster)
    main("201307")
    # client.close()
    # cluster.close()
