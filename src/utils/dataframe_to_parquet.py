def dataframe_to_parquet(df, table_name, compression: str = "snappy"):
    
    """Saves a dataframe to parquet file

    Args:
    - df: dataframe of transformed table
    - table_name: name of the dimensions /fact table
    - compression: One of ["snappy", "gzip", "brotli", "none"] user choice
    returns:
        The path of the parquet file

    """

    valid_compressions = ["snappy", "gzip", "brotli", "none"]
    if compression not in valid_compressions:
        raise ValueError(f"Invalid compression: {compression}")
    """compression can be "snappy" => Fast compression, moderate size reduction
                        "gzip"  => Higher compression ratio, slower compression/decompression
                        "brotli"=> Very good compression ratio, slower, newer
                        "none"=> No Compression, larger file size but fastest to read/ write  """
    try:
        path = f"{table_name}.parquet"
        df.to_parquet(path, engine="pyarrow", compression=compression)  #
        return path

    except Exception as e:
        print(f"Error: {e}")
        return None
