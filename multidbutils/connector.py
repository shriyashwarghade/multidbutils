"""
Creates Database Connection Objects
"""


def mssql(server_url, database_name, user, password, driver="SQL Server", autocommit=False, ansi=False, timeout=0,
          **kwargs):
    """
    :param server_url: server name or instance name
    :type server_url: str
    :param database_name: Database Name to connect
    :type database_name: str
    :param user: user to connect to DB
    :type user: str
    :param password: password for user
    :type password: str

    *Optional Parameters*
    :param driver:  Connection Server Type
    :type driver: str

    Special Keywords (With Reference To pyodbc)

      autocommit
        If False or zero, the default, transactions are created automatically as
        defined in the DB API 2.  If True or non-zero, the connection is put into
        ODBC autocommit mode and statements are committed automatically.

      ansi
        By default, pyodbc first attempts to connect using the Unicode version of
        SQLDriverConnectW.  If the driver returns IM001 indicating it does not
        support the Unicode version, the ANSI version is tried.  Any other SQLSTATE
        is turned into an exception.  Setting ansi to true skips the Unicode
        attempt and only connects using the ANSI version.  This is useful for
        drivers that return the wrong SQLSTATE (or if pyodbc is out of date and
        should support other SQLSTATEs).

      timeout
        An integer login timeout in seconds, used to set the SQL_ATTR_LOGIN_TIMEOUT
        attribute of the connection.  The default is 0 which means the database's
        default timeout, if any, is used.

      For More Details Regrading Special Keywords Please refer pyodbc Documentation (https://pypi.org/project/pyodbc/)

    :return: Returns pyodbc Cursor
    """
    try:
        import pyodbc
        connection_string = f'Driver={driver};Server={server_url};Database={database_name};' \
                            f'user={user},password={password}'
        conn = pyodbc.connect(connection_string, autocommit=autocommit, ansi=ansi, timeout=timeout, **kwargs)
        return conn.cursor()
    except Exception as e:
        raise Exception(e)


def mongo(host, database_name, port=27017, user="", password="", document_class=dict,
          tz_aware=None, connect=None, type_registry=None, **kwargs):
    """
    :param host: server url or instance name
    :type host:str
        For username and passwords reserved characters like ':', '/', '+' and '@' must be percent encoded following
        RFC 2396::
            try:
                # Python 3.x
                from urllib.parse import quote_plus
            except ImportError:
                # Python 2.x
                from urllib import quote_plus

    :param database_name: Database Name to connect
    :type host:str
    :param port: Port on which database service is running
    :param user: User to connect to database
    :param password: Password of user

    Special Keywords (With Reference To pymongo)

      - `document_class` (optional): default class to use for
        documents returned from queries on this client
      - `type_registry` (optional): instance of
        :class:`~bson.codec_options.TypeRegistry` to enable encoding
        and decoding of custom types.
      - `tz_aware` (optional): if ``True``,
        :class:`~datetime.datetime` instances returned as values
        in a document by this :class:`MongoClient` will be timezone
        aware (otherwise they will be naive)
      - `connect` (optional): if ``True`` (the default), immediately
        begin connecting to MongoDB in the background. Otherwise connect
        on the first operation.
      - `directConnection` (optional): if ``True``, forces this client to
         connect directly to the specified MongoDB host as a standalone.
         If ``false``, the client connects to the entire replica set of
         which the given MongoDB host(s) is a part. If this is ``True``
         and a mongodb+srv:// URI or a URI containing multiple seeds is
         provided, an exception will be raised.

      | **Other optional parameters can be passed as keyword arguments:**

      - `maxPoolSize` (optional): The maximum allowable number of
        concurrent connections to each connected server. Requests to a
        server will block if there are `maxPoolSize` outstanding
        connections to the requested server. Defaults to 100. Cannot be 0.
      - `minPoolSize` (optional): The minimum required number of concurrent
        connections that the pool will maintain to each connected server.
        Default is 0.
      - `maxIdleTimeMS` (optional): The maximum number of milliseconds that
        a connection can remain idle in the pool before being removed and
        replaced. Defaults to `None` (no limit).
      - `socketTimeoutMS`: (integer or None) Controls how long (in
        milliseconds) the driver will wait for a response after sending an
        ordinary (non-monitoring) database operation before concluding that
        a network error has occurred. ``0`` or ``None`` means no timeout.
        Defaults to ``None`` (no timeout).
      - `connectTimeoutMS`: (integer or None) Controls how long (in
        milliseconds) the driver will wait during server monitoring when
        connecting a new socket to a server before concluding the server
        is unavailable. ``0`` or ``None`` means no timeout.
        Defaults to ``20000`` (20 seconds).
      - `server_selector`: (callable or None) Optional, user-provided
        function that augments server selection rules. The function should
        accept as an argument a list of
        :class:`~pymongo.server_description.ServerDescription` objects and
        return a list of server descriptions that should be considered
        suitable for the desired operation.
      - `serverSelectionTimeoutMS`: (integer) Controls how long (in
        milliseconds) the driver will wait to find an available,
        appropriate server to carry out a database operation; while it is
        waiting, multiple server monitoring operations may be carried out,
        each controlled by `connectTimeoutMS`. Defaults to ``30000`` (30
        seconds).
      - `waitQueueTimeoutMS`: (integer or None) How long (in milliseconds)
        a thread will wait for a socket from the pool if the pool has no
        free sockets. Defaults to ``None`` (no timeout).
      - `waitQueueMultiple`: (integer or None) Multiplied by maxPoolSize
        to give the number of threads allowed to wait for a socket at one
        time. Defaults to ``None`` (no limit).
      - `heartbeatFrequencyMS`: (optional) The number of milliseconds
        between periodic server checks, or None to accept the default
        frequency of 10 seconds.
      - `appname`: (string or None) The name of the application that
        created this MongoClient instance. MongoDB 3.4 and newer will
        print this value in the server log upon establishing each
        connection. It is also recorded in the slow query log and
        profile collections.
      - `driver`: (pair or None) A driver implemented on top of PyMongo can
        pass a :class:`~pymongo.driver_info.DriverInfo` to add its name,
        version, and platform to the message printed in the server log when
        establishing a connection.
      - `event_listeners`: a list or tuple of event listeners. See
        :mod:`~pymongo.monitoring` for details.
      - `retryWrites`: (boolean) Whether supported write operations
        executed within this MongoClient will be retried once after a
        network error on MongoDB 3.6+. Defaults to ``True``.

      For More Details Regrading Special Keywords Please refer pymongo Documentation (https://pypi.org/project/pymongo/)

    :return: Database Connection Object
    """
    try:
        from pymongo import MongoClient
        connection_string = f'mongodb://{user}:{password}@{host}:{port}' if user and password else \
            f'mongodb://{host}:{port}'
        return MongoClient(connection_string, document_class=document_class, tz_aware=tz_aware, connect=connect,
                           type_registry=type_registry, **kwargs)[database_name]
    except Exception as e:
        raise Exception(e)
