import os

import pyodbc
import logging
from wherescape.logging import initialise_wherescape_logging


class WhereScape:
    """
    The Wherescape class takes care of interactions with the Wherescape
    framework and all database connections needed.
    """

    main_message = ""

    def __init__(self):
        """
        Wherescape class instance initialisation function. Sets and checks validity
        of the the connection strings
        from environment variables and gets information from the load table.

        Initialises:
        ------------
        meta_db_connection_string   - a connection string for the metadata database
        target_db_connection_string - a connection string for the target database
        schema, table               - schemaname, tablename from the WhereScape Context
        load_full_name              - 'schema.table' from the WhereScape Context
        column_names, column_types  - column_names, column_types of a load or stage table
        object_key                  - object_key of a load or stage table
        file_path, file_name        - Taken from the lt_file_path, lt_file_name of the Wherescape object context
        """
        self.workdir = os.getenv("WSL_WORKDIR")
        initialise_wherescape_logging(self)

        wsl_meta_dns = os.getenv("WSL_META_DSN")
        wsl_meta_user = os.getenv("WSL_META_USER")
        wsl_meta_pwd = os.getenv("WSL_META_PWD")
        self.meta_db_connection_string = (
            f"DSN={wsl_meta_dns};UID={wsl_meta_user};PWD={wsl_meta_pwd}"
        )

        wsl_tgt_dns = os.getenv("WSL_TGT_DSN")
        wsl_tgt_user = os.getenv("WSL_TGT_USER")
        wsl_tgt_pwd = os.getenv("WSL_TGT_PWD")
        self.target_db_connection_string = (
            f"DSN={wsl_tgt_dns};UID={wsl_tgt_user};PWD={wsl_tgt_pwd}"
        )

        self.sequence = os.getenv("WSL_SEQUENCE")
        self.job_key = os.getenv("WSL_JOB_KEY")
        self.job_name = os.getenv("WSL_JOB_NAME")
        self.task_key = os.getenv("WSL_TASK_KEY")
        self.task_name = os.getenv("WSL_TASK_NAME")

        self.table = os.getenv("WSL_LOAD_TABLE")
        self.schema = os.getenv("WSL_LOAD_SCHEMA")
        if self.schema == "load":
            # This script is related to a load table.
            sql = "SELECT lt_obj_key, lt_file_path, lt_file_name FROM ws_load_tab WHERE lt_table_name = ?"
            results = self.query_meta(sql, [self.table])
            self.object_key = results[0][0]
            self.file_path = results[0][1]
            self.file_name = results[0][2]
            self.load_full_name = os.getenv("WSL_LOAD_FULLNAME")
        elif self.schema == "stage":
            # This script is related to a stage table.
            sql = "SELECT st_obj_key FROM ws_stage_tab WHERE st_table_name = ?"
            results = self.query_meta(sql, [self.table])
            self.object_key = results[0][0]

    def get_columns(self):
        """
        Function to get the column names and types of the connected table.
        """
        if self.schema == "load":
            sql = "SELECT lc_col_name, lc_data_type FROM ws_load_col WHERE lc_obj_key = ? ORDER BY lc_order"
        elif self.schema == "stage":
            sql = "SELECT sc_col_name, sc_data_type FROM ws_stage_col WHERE sc_obj_key = ? ORDER BY sc_order"
        else:
            logging.warn("Invalid schema: %s" % self.schema)
            return None

        results = self.query_meta(sql, [self.object_key])
        if results:
            column_names = [result[0] for result in results]
            column_types = [result[1] for result in results]
        return (column_names, column_types)

    def query(self, conn, sql, params=[]):
        """
        Generic query function. Used for all connections
        Can only be used for SELECT queries that return one resultset
        """
        try:
            cursor = conn.cursor()
            cursor = cursor.execute(sql, params)
            # columns may be needed later.
            # columns = [column[0] for column in cursor.description]
            values = cursor.fetchall()
            conn.commit()
            cursor.close()
        except Exception as e:
            logging.error(e)
            raise
        return values

    def query_meta(self, sql, params=[]):
        """
        Query the meta database. Makes use of the generic query function.
        Can only be used for SELECT queries.

        Returns a list of tuples
        """
        try:
            conn = pyodbc.connect(self.meta_db_connection_string)
            result = self.query(conn, sql, params)
        except Exception as e:
            logging.error(e)
            raise
        return result

    def push_to_meta(self, sql, params=[]):
        """
        Function to push data to the metadate database. Returns rowcount.
        """
        try:
            conn = pyodbc.connect(self.meta_db_connection_string)
            cursor = conn.cursor()
            cursor = cursor.execute(sql, params)
            rowcount = cursor.rowcount
            conn.commit()
            cursor.close()
        except Exception as e:
            logging.error(e)
            raise
        return rowcount

    def query_target(self, sql, params=[]):
        """
        Query the target database. Makes use of the generic query function.
        """
        try:
            conn = pyodbc.connect(self.target_db_connection_string)
            result = self.query(conn, sql, params)
        except Exception as e:
            logging.error(e)
            raise
        return result

    def push_to_target(self, sql, params=[]):
        """
        Function to push data to the target database. Returns rowcount.

        Input :
        sql     : a sql statement, possibly with ? placeholders for parameters
        params  : a tuple with values to replace ? placeholders in the SQL

        Example:
        push_to_target('INSERT INTO schemaname.tablename (columname) VALUES (?)',  (value,) )

        """
        try:
            conn = pyodbc.connect(self.target_db_connection_string)
            cursor = conn.cursor()
            cursor = cursor.execute(sql, params)
            rowcount = cursor.rowcount
            conn.commit()
            cursor.close()
        except Exception as e:
            logging.error(e)
            raise
        return rowcount

    def push_many_to_target(self, sql, params=[]):
        """
        Function to push data to the target database.

        Input :
        sql     : a sql statement, possibly with ? placeholders for parameters
        params  : list of tuples with values to replace ? placeholders in the SQL

        Example:
        row1 = ('a',)
        row2 = ('b',)
        rows = [row1, row2]
        push_to_target('INSERT INTO schemaname.tablename (columname) VALUES (?)',  rows )
        """
        try:
            conn = pyodbc.connect(self.target_db_connection_string)
            conn.autocommit = False
            cursor = conn.cursor()
            cursor.executemany(sql, params)
        except Exception as e:
            conn.rollback()
            logging.error(e)
            raise
        else:
            conn.commit()
            cursor.close()

    def read_parameter(self, name, include_comment=False):
        """
        Function to read a parameter from Wherescape.
        """
        # Intialize return values
        result = ""
        comment = ""

        sql = """
            DECLARE @out varchar(max),@out1 varchar(max);
            EXEC WsParameterRead
        	  @p_parameter = ?
            ,@p_value = @out OUTPUT
            ,@p_comment=@out1 OUTPUT
            SELECT @out AS p_value,@out1 AS p_comment;"""

        try:
            result = self.query_meta(sql, [name])
        except Exception as e:
            logging.error(e)
            raise
        else:
            if len(result) == 0:
                parameter = ""
            else:
                parameter = result[0][0]
                comment = result[0][1]

        if include_comment:
            return parameter, comment
        else:
            return parameter

    def write_parameter(self, name, value="", comment=None):
        """
        Function to update or insert a parameter into Wherescape.

        Returns result number:
        1 Metadata Parameter Updated
        2 Metadata Parameter Added
        -3 Fatal/Unexpected Error
        """
        sql = """
        DECLARE @out INT;
        EXEC  @out=WsParameterWrite
          @p_parameter = ?
        , @p_value = ?
        , @p_comment  = ?
        SELECT @out AS return_value;"""
        result_number = self.push_to_meta(sql, [name, value, comment])
        result_number = int(result_number)
        return result_number
