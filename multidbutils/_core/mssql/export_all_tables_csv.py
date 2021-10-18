import csv
import os
import sys
from datetime import datetime

from tqdm import tqdm

from multidbutils._core.functions.msql_functions import get_table_schema, \
    get_all_tables_names_list


def export_all_tables_to_csv(sql_connection_cursor, delimiter, quotechar, newline, encoding, export_path):
    if export_path:
        if not os.path.exists(export_path):
            raise Exception(f"{export_path} No Such Directory Found.")
        export_path_to_return = export_path
    else:
        file_path = os.path.dirname(sys.argv[0])
        csv_export_path = os.path.join(file_path, "csv_export", )
        if not os.path.exists(csv_export_path):
            os.mkdir(csv_export_path)
        datetime_now = str(datetime.timestamp(datetime.now())).replace('.', '')
        export_path = os.path.join(csv_export_path, datetime_now)
        os.mkdir(export_path)
        export_path_to_return = file_path + "/" + "csv_export" + "/" + datetime_now
    start_time = datetime.now()
    count = 0
    source_tables_list = get_all_tables_names_list(sql_connection_cursor)
    for table in tqdm(range(len(source_tables_list)), desc="Processing Tables"):
        table_name = source_tables_list[table]
        with open(os.path.join(export_path, table_name + ".csv"), mode='w', newline=newline,
                  encoding=encoding) as table_file:
            table_writer = csv.writer(table_file, delimiter=delimiter, quotechar=quotechar)
            table_schema = get_table_schema(sql_connection_cursor, table_name, skip_type=True)
            table_writer.writerow(table_schema)
            sql_data = sql_connection_cursor.execute(f"SELECT * from {table_name}")
            for data in sql_data:
                data_to_insert = []
                for col in range(len(table_schema)):
                    data_to_insert.append(str(data[col]))
                table_writer.writerow(data_to_insert)
                count += 1
    print(
        f"Total {count} Records From {len(source_tables_list)} Tables Exported From MS-SQL To CSV On "
        f"{export_path_to_return} path In {datetime.now() - start_time}")
    return export_path_to_return
