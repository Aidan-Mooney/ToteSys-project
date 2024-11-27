def query_db(
    sql_string: str,
    connect_to_db,
    close_db_connection,
    dict_name: str = "response",
    secret_name: str = "totesys_db_credentials",
    **kwargs: dict,
):
    """
    Take an sql query string and return the result of the query as a dictionary formatted like a json object with table names etc.
    If dict_name = "", only return the first row of the query response as a dictionary with column keys.
    Otherwise, return a dictionary containing a list of dictionaries, where each dictionary contains a row of the query response.

    :param sql_string: str string containing valid PostgreSQL query
    :param connect_to_db: function which returns connection to a database
    :param close_db_connection: function which closes database connection
    :param dict_name: name used in the key of the reponse dictionary
    :param secret_name: name of secret stored in AWS Secrets Manager
    :param kwargs: keys and values passed into SQL query using :-syntax
    """
    conn = connect_to_db(secret_name)
    db_query = conn.run(sql_string, **kwargs)
    if not conn.columns:
        return None
    cols = [col["name"] for col in conn.columns]
    close_db_connection(conn)
    if dict_name:
        return {dict_name: [dict(zip(cols, row)) for row in db_query]}
    return dict(zip(cols, db_query[0]))
