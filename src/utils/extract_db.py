from db.connection import connect_to_db, close_db


def extract_db(table_name):
    """Lambda function that extracts data from a database table and returns it as a formatted dictionary

    Args:
        table_name (string): name of the dbtable to extract data from

    Returns:
        dict: dictionary with table name as the key and a list  value containing each table row as a dict
    """

    try:
        conn = connect_to_db(".env.dev")
        query = f"SELECT * FROM {table_name}"
        response = conn.run(query)
        columns = [column["name"] for column in conn.columns]
        table_dict = {table_name: [dict(zip(columns, row)) for row in response]}
        return table_dict

    except Exception as error:
        print(f"Failed to extract from DB: {error}")
        raise error
    finally:
        if conn:
            close_db(conn)
