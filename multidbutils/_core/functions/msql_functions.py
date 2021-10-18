# @lru_cache()
def convert_sql_data_to_python(data_dict):
    from decimal import Decimal
    if data_dict is None:
        return None
    for k, v in list(data_dict.items()):
        if isinstance(v, dict):
            convert_sql_data_to_python(v)
        elif isinstance(v, list):
            for l in v:
                convert_sql_data_to_python(l)
        elif isinstance(v, Decimal):
            data_dict[k] = str(v)
        elif isinstance(v, bytes):
            data_dict[k] = str(v)


def get_all_tables_names_list(sql_connection_cursor):
    tables_cursor = sql_connection_cursor.execute(
        "SELECT SCHEMA_NAME(schema_id)+'.'+name AS SchemaTable FROM sys.tables")
    tables_list = [str(t_name[0]) for t_name in tables_cursor]
    return tables_list


def get_table_schema(sql_connection_cursor, table_name, skip_type=False):
    table_schema = [schema.column_name if skip_type else (schema.column_name, schema.type_name) for schema in
                    sql_connection_cursor.columns(table=table_name.split(".")[1])]
    return table_schema


def check_table_names(sql_connection_cursor, source_tables_list):
    default_source_tables_list = get_all_tables_names_list(sql_connection_cursor)
    return set(source_tables_list) - set(default_source_tables_list)
