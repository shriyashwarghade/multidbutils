from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(
    title="multidbutils",
    version="1.0.0",
    docs_url='/'
)


# Export APIs
@app.get("/export/mssql/all-tables-to-csv", tags=['Export'])
def ms_sql_all_tables_to_csv(server_url, database_name, user, password, export_path="", delimiter=',', quotechar='"',
                             newline='', encoding="utf-8", ):
    try:
        from multidbutils.mssql import all_tables_to_csv as ms_sql_all_tables_to_csv
        path = ms_sql_all_tables_to_csv(server_url=server_url, database_name=database_name, user=user,
                                        password=password, export_path=export_path, delimiter=delimiter,
                                        quotechar=quotechar, newline=newline, encoding=encoding)
        return JSONResponse(status_code=200, content=f"Tables Exported From MS-SQL To CSV On {path} path")
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content=f"Error:{e}")


@app.get("/export/mssql/selected-tables-to-csv", tags=['Export'])
def ms_sql_selected_tables_to_csv(server_url, database_name, user, password, tables_name, export_path="", delimiter=',',
                                  quotechar='"', newline='', encoding="utf-8", ):
    try:
        from multidbutils.mssql import tables_to_csv
        path = tables_to_csv(tables_name.split(","), server_url=server_url, database_name=database_name, user=user,
                             password=password, export_path=export_path, delimiter=delimiter, quotechar=quotechar,
                             newline=newline, encoding=encoding)
        return JSONResponse(status_code=200, content=f"Tables Exported From MS-SQL To CSV On {path} path")
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content=f"Error:{e}")


@app.get("/export/mssql/all-tables-to-json", tags=['Export'])
def ms_sql_all_tables_to_json(server_url, database_name, user, password, export_path=""):
    try:
        from multidbutils.mssql import all_tables_to_json
        path = all_tables_to_json(server_url=server_url, database_name=database_name, user=user,
                                  password=password, export_path=export_path)
        return JSONResponse(status_code=200, content=f"Tables Exported From MS-SQL To Json On {path} path")
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content=f"Error:{e}")


@app.get("/export/mssql/selected-tables-to-json", tags=['Export'])
def ms_sql_selected_tables_to_json(server_url, database_name, user, password, tables_name, export_path=""):
    try:
        from multidbutils.mssql import tables_to_json
        path = tables_to_json(tables_name.split(","), server_url=server_url, database_name=database_name, user=user,
                              password=password, export_path=export_path)
        return JSONResponse(status_code=200, content=f"Tables Exported From MS-SQL To Json On {path} path")
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content=f"Error:{e}")


# Sync APIs
@app.get("/sync/mssql/all-tables-to-mongo", tags=['Sync'])
def ms_sql_all_tables_to_mongo(sql_server_url, sql_database_name, sql_user, sql_password, mongo_server_url,
                               mongo_database_name, mongo_user="", mongo_password="", mongo_server_port="27017",
                               failure_rollback=True, flush_old_data=True, ):
    try:
        from multidbutils.mssql import all_tables_to_mongo
        all_tables_to_mongo(sql_server_url=sql_server_url, sql_database_name=sql_database_name, sql_user=sql_user,
                            sql_password=sql_password, mongo_server_url=mongo_server_url,
                            mongo_database_name=mongo_database_name, mongo_server_port=mongo_server_port,
                            mongo_user=mongo_user, mongo_password=mongo_password, failure_rollback=failure_rollback,
                            flush_old_data=flush_old_data)
        return JSONResponse(status_code=200, content=f"Tables Sync To Mongo Completed")
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content=f"Error:{e}")


@app.get("/sync/mssql/selected-tables-to-mongo", tags=['Sync'])
def ms_sql_selected_tables_to_mongo(sql_server_url, sql_database_name, sql_user, sql_password, tables_name,
                                    mongo_server_url, mongo_database_name, mongo_user="", mongo_password="",
                                    mongo_server_port="27017", failure_rollback=True, flush_old_data=True, ):
    try:
        from multidbutils.mssql import tables_to_mongo
        tables_to_mongo(tables_name=tables_name.split(','), sql_server_url=sql_server_url,
                        sql_database_name=sql_database_name,
                        sql_user=sql_user, sql_password=sql_password, mongo_server_url=mongo_server_url,
                        mongo_database_name=mongo_database_name, mongo_server_port=mongo_server_port,
                        mongo_user=mongo_user, mongo_password=mongo_password, failure_rollback=failure_rollback,
                        flush_old_data=flush_old_data)
        return JSONResponse(status_code=200, content=f"Tables Sync To Mongo Completed")
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content=f"Error:{e}")
