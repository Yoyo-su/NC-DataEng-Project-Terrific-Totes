from db.connection import connect_to_db, close_db

def extract_db(table_name):
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

        
