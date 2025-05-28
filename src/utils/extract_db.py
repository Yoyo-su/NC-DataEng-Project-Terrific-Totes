from db.connection import connect_to_db, close_db


def extract_db(table_name):
    """Lambda function that extracts data from a database table and returns it as a formatted dictionary

    Args:
        table_name (string): table to extract data from

    Returns:
        _type_: _description_
    """

    try:
        conn = connect_to_db(".env.totesys")
        query = f"SELECT * FROM {table_name}"
        response = conn.run(query)
        columns = [column["name"] for column in conn.columns]
        table_dict = {table_name: [dict(zip(columns, row)) for row in response]}
        return table_dict

    except Exception as error:
        print(f"Failed to extract from DB: {error}")

    finally:
        if conn:
            close_db(conn)
