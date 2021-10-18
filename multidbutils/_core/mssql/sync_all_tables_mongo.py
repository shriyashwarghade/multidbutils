from datetime import datetime

from tqdm import tqdm

from multidbutils._core.functions.mongo_functions import get_all_collection_names_list
from multidbutils._core.functions.msql_functions import convert_sql_data_to_python, get_table_schema, \
    get_all_tables_names_list


def sync_all_tables_to_mongo(sql_connection_cursor, mongo_connection_cursor, failure_rollback, flush_old_data):
    start_time = datetime.now()
    count = 0
    source_tables_list = get_all_tables_names_list(sql_connection_cursor)
    destination_tables_list = get_all_collection_names_list(mongo_connection_cursor)
    rollback_tables = []
    new_tables = []
    try:
        for table in tqdm(range(len(source_tables_list)), desc="Processing Tables"):
            table_name = source_tables_list[table]
            if failure_rollback and table_name in destination_tables_list:
                new_tables.append(table_name)
                new_table_name = f"{table_name}_multidbsynautobackup_" \
                                 f"{str(datetime.timestamp(datetime.now())).replace('.', '')}"
                rollback_tables.append((table_name, new_table_name))
                if flush_old_data:
                    mongo_connection_cursor[table_name].rename(new_table_name)
                else:
                    temp_cursor = mongo_connection_cursor[table_name].find()
                    for data in temp_cursor:
                        mongo_connection_cursor[new_table_name].insert(data)
            table_schema = get_table_schema(sql_connection_cursor, table_name)
            sql_data = sql_connection_cursor.execute(f"SELECT * from {table_name}")
            if flush_old_data: mongo_connection_cursor[table_name].remove({})
            for data in sql_data:
                data_to_insert = {}
                for col in range(len(table_schema)):
                    data_to_insert.update({table_schema[col][0]: data[col]})
                convert_sql_data_to_python(data_to_insert)
                count += 1
                mongo_connection_cursor[table_name].insert(data_to_insert)
        for table in rollback_tables:
            mongo_connection_cursor[table[1]].drop()
        print(
            f"Total {count} Records From {len(source_tables_list)} Tables Synced From MS-SQL To Mongo "
            f"In {datetime.now() - start_time}")
    except:
        import traceback
        traceback.print_exc()
        if failure_rollback:
            for table in new_tables:
                mongo_connection_cursor[table].drop()
            for table in rollback_tables:
                mongo_connection_cursor[table[1]].rename(table[0])
