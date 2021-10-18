def get_all_collection_names_list(mongo_connection_cursor):
    collection_name_list = [str(t_name) for t_name in mongo_connection_cursor.list_collection_names()]
    return collection_name_list
