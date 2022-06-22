"""Interact with a database"""

import getpass
import keyring
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import pandas
from datamanipy.database_info import DbConnInfoStore


class Database():

    """
    A class used to represent a database.

    Attributes
    ----------
    db_key : str, optional
        Key identifying a database stored in your database connection information storage (see class DbConnInfoStorage)
    scheme : {'postgresql', 'sqlite', 'mysql', 'oracle' or 'mssql'}, optional
        Dialect used to connect to the database
    host : str, optional
        Address of the database server
    port : str, optional
        TCP port number of the database server
    name : str, optional
        Database name
    user : str, optional
        Database username
    uri : str
        Database URI
    application_name : str, default 'MyPythonApp'
        Name of your application. It will allows you to retrieve your connection in the database
    """

    def __init__(self, db_key=None, scheme=None, host=None, port=None, name=None, user=None, application_name='MyPythonApp'):
        self.db_key = db_key
        self.application_name = application_name
        if self.db_key is None:
            self.db_key = db_key
            self.scheme = scheme
            self.host = host
            self.port = port
            self.name = name
            self.user = user
            self.uri = scheme + '://' + user + ':********@' + host + ':' + port + '/' + name
        else:  # get database information from storage if db_key is given
            db_storage = DbConnInfoStore()
            db = db_storage.get_db(db_key)
            self.scheme = db['scheme']
            self.host = db['host']
            self.port = db['port']
            self.name = db['name']
            self.user = db['user']
            self.uri = db['uri']
        self.engine = self.create_engine()

    def _split_schema_from_table_name(self, table):
        """Split schema and table names"""
        name = table.split('.')[-1]
        try:
            schema = table.split('.')[-2]
        except:
            schema = None
        return schema, name

    def _get_password(self):
        """Get user password in credential manager"""
        password = keyring.get_password(
            self.uri, self.user)  # try to get password from credential manager
        if password is None:
            raise ValueError
        return password

    def _ask_password(self):
        """Ask for user password"""
        return getpass.getpass(
            prompt=f'Password for {self.uri}: ')  # ask for password password

    def _password(self):
        try:
            return self._get_password()
        except:
            return self._ask_password()

    def _save_password(self, password):
        """Save password in credential manager if not already saved"""
        try:  # check if password is already saved
            pwd_already_saved = (password == self._get_password())
        except:
            pwd_already_saved = False
        if pwd_already_saved == False:  # if password is not already saved
            # ask to the user if he wants to save password
            save_pwd = input('Save password? (y/n): ')
            if save_pwd == 'y':
                keyring.set_password(
                    service_name=self.uri, username=self.user, password=password)  # save password in credential manager

    def reset_password(self):
        """Reset password"""
        password = self._ask_password()
        keyring.set_password(
            service_name=self.uri, username=self.user, password=password)  # save password in credential manager

    def show_info(self):
        """Show database connection information"""
        print(f'db_key: {self.db_key}')
        print(f'scheme: {self.scheme}')
        print(f'host: {self.host}')
        print(f'port: {self.port}')
        print(f'name: {self.name}')
        print(f'user: {self.user}')
        print(f'uri: {self.uri}')

    def create_engine(self):
        """Create an engine"""
        password = self._password()
        engine = create_engine(
            self.uri.replace("********", password),
            executemany_mode='values_only',
            connect_args={"application_name": self.application_name})
        with engine.connect() as con:
            self._save_password(password)
        print('Engine operational. Close it with the close_engine() method.')
        return engine
    
    def close_engine(self):
        """Close the engine"""
        self.engine.dispose()

    def session(self):
        """Create a database session
        Why use a session ? https://stackoverflow.com/questions/34322471/sqlalchemy-engine-connection-and-session-difference
        """
        return sessionmaker(bind=self.engine, autocommit=True)

    def connect(self):
        """Connect to database"""
        return self.engine.begin()

    def set_role(self, role):
        """Enable roles for the current session
        
        Parameters
        ----------
        role : str
            Name of the role to use for the current session

        Returns
        -------
        None
        """
        self.execute(f"""SET ROLE {role};""")

    def table(self, table):
        """Create an object representing a table in the database
        
        Parameters
        ----------
        table : str
            Name of the table, preceeding by its schema (schema.table)
        
        Returns
        -------
        sqlalchemy.schema.Table
        """
        schema, name = self._split_schema_from_table_name(table)
        return Table(name, MetaData(), schema=schema, autoload=True, autoload_with=self.engine)

    def comment_table(self, table, comment):
        """Comment a table
        
        Parameters
        ----------
        table : str
            Name of the table, preceeded by its schema (schema.table)
        comment : str
            Comment for the table

        Returns
        -------
        None
        """
        comment = comment.replace("\'", "\'\'")
        self.execute(f"""COMMENT ON TABLE {table} IS '{comment}';""")

    def comment_column(self, table, column, comment):
        """Comment a column
        
        Parameters
        ----------
        table : str
            Name of the table, preceeded by its schema (schema.table)
        column : str
            Name of the column
        comment : str
            Comment for the table

        Returns
        -------
        None
        """
        comment = comment.replace("\'", "\'\'")
        self.execute(f"""COMMENT ON COLUMN {table}.{column} IS '{comment}';""")

    def to_df(self, sql):
        """Import data from database into a pandas.DataFrame

        Parameters
        ----------
        sql : str
            SQL query

        Returns
        -------
        pandas.DataFrame
        """
        with self.connect() as con:
            return pandas.read_sql(sql=sql, con=con)

    def get_postgre_table_metadata(self, table):
        """Import table metadata into a pandas.DataFrame

        Parameters
        ----------
        table : str
            Name of the table, preceeded by its schema (schema.table)

        Returns
        -------
        pandas.DataFrame
        """
        schema, name = self._split_schema_from_table_name(table)
        if schema is None:
            schema = 'public'
        sql = f"SELECT column_name, data_type, is_nullable, pg_catalog.col_description(format('{schema}.{name}',isc.table_schema,isc.table_name)::regclass::oid,isc.ordinal_position) FROM information_schema.columns isc WHERE table_schema = '{schema}' AND table_name = '{name}'"
        return self.to_df(sql)

    def insert_df(self, df, table, if_exists='fail', index=False, index_label=None, chunksize=None, dtype=None):
        """Insert a pandas.DataFrame into the database

        Parameters
        ----------
        table : str
            Name of the table, preceeded by its schema (schema.table)
        if_exists : {'fail', 'replace', 'append'}, default 'append'
            How to behave if the table already exists.
            - fail: raise a ValueError
            - replace: drop the table before inserting new values
            - append: insert new values to the existing table
        index : bool, default True
            Write DataFrame index as a column. Uses index_label as the column name in the table.
        index_label : str or sequence, default None
            Column label for index column(s). If None is given (default) and index is True, then the index names are used. A sequence should be given if the DataFrame uses MultiIndex.
        chunksize : int, optional
            Specify the number of rows in each batch to be written at a time. By default, all rows will be written at once.
        dtype : dict or scalar, optional
            Specifying the datatype for columns. If a dictionary is used, the keys should be the column names and the values should be the SQLAlchemy types or strings for the sqlite3 legacy mode. If a scalar is provided, it will be applied to all columns.

        Returns
        -------
        None
        """
        schema, name = self._split_schema_from_table_name(table)
        with self.connect() as con:
            df.to_sql(name=name, schema=schema, con=con, if_exists=if_exists, index=index,
                      index_label=index_label, chunksize=chunksize, dtype=dtype)

    def execute(self, sql):
        """Execute a SQL query

        Parameters
        ----------
        sql : str or sqlalchemy.sql.expression.Select
            SQL query

        Returns
        -------
        sqlalchemy.engine.cursor.LegacyCursorResult
        """
        if type(sql) == str:
            sql = text(sql)
        with self.connect() as con:
            results = con.execute(sql)
        return results

    def execute_file(self, sql_file):
        """Execute a SQL query stored in a file

        Parameters
        ----------
        sql : str
            SQL file

        Returns
        -------
        sqlalchemy.engine.cursor.LegacyCursorResult
        """
        with open(sql_file, 'r') as sql_wrapper:
            sql_query = sql_wrapper.read()
            results = self.execute(sql_query)
        return results
