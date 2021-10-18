import json
import json
import os
import sys
from datetime import datetime

from tqdm import tqdm

from multidbutils._core.functions.msql_functions import convert_sql_data_to_python, get_table_schema, \
    check_table_names


def export_tables_to_json(sql_connection_cursor, source_tables_list, export_path, skipkeys, ensure_ascii,
                          check_circular, allow_nan, cls, indent, separators, default, sort_keys, **kw):
    if isinstance(source_tables_list, str): source_tables_list = [source_tables_list]
    tables_not_found = check_table_names(sql_connection_cursor, source_tables_list)
    if tables_not_found:
        raise Exception(f"{','.join(tables_not_found)} Table(s) Not Found in MSSQL Database.")
    if export_path:
        if not os.path.exists(export_path):
            raise Exception(f"{export_path} No Such Directory Found.")
        export_path_to_return = export_path
    else:
        file_path = os.path.dirname(sys.argv[0])
        csv_export_path = os.path.join(file_path, "json_export")
        if not os.path.exists(csv_export_path): os.mkdir(csv_export_path)
        datetime_now = str(datetime.timestamp(datetime.now())).replace('.', '')
        export_path = os.path.join(csv_export_path, datetime_now)
        os.mkdir(export_path)
        export_path_to_return = file_path + "/" + "json_export" + "/" + datetime_now

    start_time = datetime.now()
    count = 0
    for table in tqdm(range(len(source_tables_list)), desc="Processing Tables"):
        table_name = source_tables_list[table]
        add_comma = False
        with open(os.path.join(export_path, table_name + ".json"), "w") as table_file:
            table_file.write("[")
            table_schema = get_table_schema(sql_connection_cursor, table_name)
            sql_data = sql_connection_cursor.execute(f"SELECT * from {table_name}")
            for data in sql_data:
                data_to_insert = {}
                for col in range(len(table_schema)):
                    if table_schema[col][1] == 'datetime':
                        data_to_insert.update({table_schema[col][0]: data[col].isoformat()})
                    else:
                        data_to_insert.update({table_schema[col][0]: data[col]})
                convert_sql_data_to_python(data_to_insert)
                if add_comma: table_file.write(",")
                json.dump(data_to_insert, table_file, skipkeys=skipkeys, ensure_ascii=ensure_ascii,
                          check_circular=check_circular, allow_nan=allow_nan, cls=cls, indent=indent,
                          separators=separators, default=default, sort_keys=sort_keys, **kw)
                add_comma = True
                count += 1
            table_file.write("]")
    print(
        f"Total {count} Records From {len(source_tables_list)} Tables Exported From MS-SQL To JSON On "
        f"{export_path_to_return} path In {datetime.now() - start_time}")
    return export_path_to_return
