from multidbutils.connector import mssql, mongo


def all_tables_to_csv(sql_connection_cursor=None, server_url="", database_name="", user="", password="", delimiter=',',
                      quotechar='"', newline='', encoding="utf-8", export_path="", **kwargs):
    if not sql_connection_cursor and not server_url:
        raise Exception("Either pass MS SQL Connector or Server Host IP.")
    if not sql_connection_cursor:
        sql_connection_cursor = mssql(server_url, database_name, user, password, **kwargs)
    from multidbutils._core.mssql.export_all_tables_csv import export_all_tables_to_csv
    return export_all_tables_to_csv(sql_connection_cursor, delimiter=delimiter, quotechar=quotechar, newline=newline,
                                    encoding=encoding, export_path=export_path)


def tables_to_csv(source_tables_list, sql_connection_cursor=None, server_url="", database_name="", user="",
                  password="", delimiter=',', quotechar='"', newline='', encoding="utf-8", export_path="", **kwargs):
    if not sql_connection_cursor and not server_url:
        raise Exception("Either pass MS SQL Connector or Server Host IP.")
    if not sql_connection_cursor:
        sql_connection_cursor = mssql(server_url, database_name, user, password, **kwargs)
    from multidbutils._core.mssql.export_tables_csv import export_tables_to_csv
    return export_tables_to_csv(sql_connection_cursor, source_tables_list=source_tables_list, delimiter=delimiter,
                                quotechar=quotechar, newline=newline, encoding=encoding, export_path=export_path)


def all_tables_to_json(sql_connection_cursor=None, server_url="", database_name="", user="", password="",
                       export_path="", skipkeys=False, ensure_ascii=True, check_circular=True, use_decimal=True,
                       allow_nan=True, cls=None, indent=None, separators=None, default=None, sort_keys=False, **kwargs):
    if not sql_connection_cursor and not server_url:
        raise Exception("Either pass MS SQL Connector or Server Host IP.")
    if not sql_connection_cursor:
        sql_connection_cursor = mssql(server_url, database_name, user, password, **kwargs)
    from multidbutils._core.mssql.export_all_tables_json import export_all_tables_to_json
    return export_all_tables_to_json(sql_connection_cursor, export_path=export_path, skipkeys=skipkeys,
                                     ensure_ascii=ensure_ascii, check_circular=check_circular, allow_nan=allow_nan,
                                     cls=cls, indent=indent, separators=separators, default=default,
                                     sort_keys=sort_keys, use_decimal=use_decimal, **kwargs)


def tables_to_json(source_tables_list, sql_connection_cursor=None, server_url="", database_name="", user="",
                   password="", export_path="", skipkeys=False, ensure_ascii=True, check_circular=True, allow_nan=True,
                   cls=None, indent=None, separators=None, default=None,
                   sort_keys=False, **kwargs):
    if not sql_connection_cursor and not server_url:
        raise Exception("Either pass MS SQL Connector or Server Host IP.")
    if not sql_connection_cursor:
        sql_connection_cursor = mssql(server_url, database_name, user, password, **kwargs)
    from multidbutils._core.mssql.export_tables_json import export_tables_to_json
    return export_tables_to_json(sql_connection_cursor, source_tables_list, export_path=export_path, skipkeys=skipkeys,
                                 ensure_ascii=ensure_ascii, check_circular=check_circular, allow_nan=allow_nan, cls=cls,
                                 indent=indent, separators=separators, default=default, sort_keys=sort_keys, **kwargs)


def all_tables_to_mongo(sql_connection_cursor=None, sql_server_url="", sql_database_name="", sql_user="",
                        sql_password="", mongo_connection_cursor=None, mongo_server_url="", mongo_database_name="",
                        mongo_server_port="27017", mongo_user="", mongo_password="", failure_rollback=True,
                        flush_old_data=True, **kwargs):
    if not sql_connection_cursor and not sql_server_url:
        raise Exception("Either pass MS SQL Connector or Server Host IP.")
    if not mongo_connection_cursor and not mongo_server_url:
        raise Exception("Either pass Mongo Connector (pyMongo) or Server Host IP.")

    if not sql_connection_cursor:
        sql_connection_cursor = mssql(sql_server_url, sql_database_name, sql_user, sql_password, **kwargs)

    if not mongo_connection_cursor:
        mongo_connection_cursor = mongo(mongo_server_url, mongo_database_name, int(mongo_server_port), mongo_user,
                                        mongo_password, **kwargs)

    from multidbutils._core.mssql.sync_all_tables_mongo import sync_all_tables_to_mongo
    sync_all_tables_to_mongo(sql_connection_cursor, mongo_connection_cursor, failure_rollback, flush_old_data)


def tables_to_mongo(tables_name, sql_connection_cursor=None, sql_server_url="", sql_database_name="", sql_user="",
                    sql_password="", mongo_connection_cursor=None, mongo_server_url="", mongo_database_name="",
                    mongo_server_port="27017", mongo_user="", mongo_password="", failure_rollback=True,
                    flush_old_data=True, **kwargs):
    if not sql_connection_cursor and not sql_server_url:
        raise Exception("Either pass MS SQL Connector or Server Host IP.")
    if not mongo_connection_cursor and not mongo_server_url:
        raise Exception("Either pass Mongo Connector (pyMongo) or Server Host IP.")

    if not sql_connection_cursor:
        sql_connection_cursor = mssql(sql_server_url, sql_database_name, sql_user, sql_password, **kwargs)

    if not mongo_connection_cursor:
        mongo_connection_cursor = mongo(mongo_server_url, mongo_database_name, int(mongo_server_port), mongo_user,
                                        mongo_password, **kwargs)

    from multidbutils._core.mssql.sync_tables_to_mongo import sync_tables_to_mongo
    sync_tables_to_mongo(sql_connection_cursor, mongo_connection_cursor, tables_name, failure_rollback,
                         flush_old_data)
