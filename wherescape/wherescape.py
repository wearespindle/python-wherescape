import logging
import os

import pyodbc

from .logging import initialise_wherescape_logging


class WhereScape:
    """
    The Wherescape class takes care of interactions with the Wherescape
    framework and all database connections needed.
    """

    # class construction variables

    # main_message is used to store the main message
    #   This is the message that appears in the job_log
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
        source_base_url             - base url of the source connection configuration
        source_user                 - user name for the source connection configuration
        source_apikey               - api key for the source connection configuration
        """  # noqa: E501
        self.workdir = os.getenv("WSL_WORKDIR")
        if self.workdir is None:
            # The WhereScape object is initialised outside of a job
            self.workdir = os.path.join("C:", os.sep, "Temp")
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
            f"DSN={wsl_tgt_dns};UID={wsl_tgt_user};PWD={wsl_tgt_pwd};sslmode=prefer"
        )

        wsl_src_dns = os.getenv("WSL_SRC_DSN")
        wsl_src_user = os.getenv("WSL_SRC_USER")
        wsl_src_pwd = os.getenv("WSL_SRC_PWD")
        self.source_db_connection_string = (
            f"DSN={wsl_src_dns};UID={wsl_src_user};PWD={wsl_src_pwd};sslmode=prefer"
        )

        self.sequence = os.getenv("WSL_SEQUENCE")
        self.job_key = os.getenv("WSL_JOB_KEY")
        self.job_name = os.getenv("WSL_JOB_NAME")
        if self.job_name is None:
            # The WhereScape object is initialised outside of a job
            self.job_name = "job"
        self.task_key = os.getenv("WSL_TASK_KEY")
        self.task_name = os.getenv("WSL_TASK_NAME")

        # source configuration; None if not set
        self.source_base_url = os.getenv("WSL_SRCCFG_URL")
        self.source_user = os.getenv("WSL_SRCCFG_USER")
        self.source_apikey = os.getenv("WSL_SRCCFG_APIKEY")

        # common input paramter list : these fields provide context for logging to the audit log  # noqa: E501
        self.common_input_parameter_list = [
            self.sequence,
            self.job_name,
            self.task_name,
            self.job_key,
            self.task_key,
        ]

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
            logging.warning("Invalid schema: %s", self.schema)
            # returning same amount removes problems showing in IDE
            return (None, None)

        results = self.query_meta(sql, [self.object_key])
        if results:
            column_names = [result[0] for result in results]
            column_types = [result[1] for result in results]
        return (column_names, column_types)

    def query(self, conn, sql, params=None):
        """
        Generic query function. Used for all connections
        Can only be used for SELECT queries that return one resultset.
        """
        if params is None:
            params = []

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

    def query_meta(self, sql, params=None):
        """
        Query the meta database. Makes use of the generic query function.
        Can only be used for SELECT queries.

        Returns a list of tuples
        """
        if params is None:
            params = []

        try:
            conn = pyodbc.connect(self.meta_db_connection_string)
            result = self.query(conn, sql, params)
        except Exception as e:
            logging.error(e)
            raise
        return result

    def push_to_meta(self, sql, params=None):
        """
        Function to push data to the metadate database. Returns rowcount.
        """
        if params is None:
            params = []

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

    def query_target(self, sql, params=None):
        """
        Query the target database. Makes use of the generic query function.
        """
        if params is None:
            params = []

        try:
            conn = pyodbc.connect(self.target_db_connection_string)
            result = self.query(conn, sql, params)
        except Exception as e:
            logging.error(e)
            raise
        return result

    def push_to_target(self, sql, params=None):
        """
        Function to push data to the target database. Returns rowcount.

        Input :
        sql     : a sql statement, possibly with ? placeholders for parameters
        params  : a tuple with values to replace ? placeholders in the SQL

        Example:
        push_to_target('INSERT INTO schemaname.tablename (columname) VALUES (?)',  (value,) )

        """
        if params is None:
            params = []

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

    def push_many_to_target(self, sql, params=None):
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
        if params is None:
            params = []

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
    
    def query_source(self, sql, params=[]):
        """
        Query a source database. Makes use of the generic query function.

        Returns a list of tuples
        """
        try:
            conn = pyodbc.connect(self.source_db_connection_string)
            result = self.query(conn, sql, params)
        except Exception as e:
            logging.error(e)
            raise
        return result
    
    def push_to_source(self, sql, params=[]):
        """
        Function to push data to a source database. Returns rowcount.

        Input :
        sql     : a sql statement, possibly with ? placeholders for parameters
        params  : a tuple with values to replace ? placeholders in the SQL

        Example:
        push_to_source('INSERT INTO schemaname.tablename (columname) VALUES (?)',  (value,) )

        """
        try:
            conn = pyodbc.connect(self.source_db_connection_string)
            cursor = conn.cursor()
            cursor = cursor.execute(sql, params)
        except Exception as e:
            rowcount=0
            conn.rollback()
            logging.error(e)
            raise
        else:
            rowcount = cursor.rowcount
            conn.commit()
            cursor.close()
        return rowcount

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
            SELECT @out AS p_value,@out1 AS p_comment;"""  # noqa: E101

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

        # else:
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

    def job_clear_logs_by_date(self, days_to_retain=90, job_to_clean="%"):
        """
        Archives job logs that are older than the specified age in days.

        Input:
        ------
        job_to_clean    : name of the job(s) to clean (Wild cards are supported)
                            The name of the job(s) whose current logs are to be archived.
                            Specifying % will match ALL jobs.
        days_to_retain  : number of days to retain the logs for the specified job(s)
                            If 90 days are retained then all the archived logs that are older than 90 days are
                            archived. If 0 days are retained then all the logs are archived.
        Returns:
        return_code     : Output Return Code:
                            • S - Success.
                            • E - Error.
                            • F - Fatal/Unexpected Error.
        return_message  : Output message indicating the action applied or the reason for no action.
        result_number:  : Output Result number:
                            •  1 Success
                            • -2 Error : Error. e.g. Due to invalid job name or job not running
                            • -3 Fatal/Unexpected Error

        """
        function_parameter_list = [job_to_clean, days_to_retain]

        sql = """
        DECLARE @out nvarchar(max),@out1 nvarchar(max),@out2 nvarchar(max);
        EXEC Ws_Job_Clear_Logs_By_Date
         @p_sequence  =?
        , @p_job_name  = ?
        , @p_task_name  = ?
        , @p_job_id = ?
        , @p_task_id = ?
        , @p_job_to_clean = ?
        , @p_day_count = ?
        , @p_return_code = @out OUTPUT
        , @p_return_msg = @out1 OUTPUT
        , @p_result   = @out2 OUTPUT;
        SELECT @out AS return_code,@out1 AS return_msg,@out2 AS return_result"""

        return_values = self.query_meta(
            sql, self.common_input_parameter_list + function_parameter_list
        )
        return_code = return_values[0][0]
        return_message = return_values[0][1]
        result_number = int(return_values[0][2])

        return return_code, return_message, result_number

    def job_clear_archive_by_date(self, days_to_retain=365, job_to_clean="%"):
        """
        Purges archives that are older than the specified age in days.

        Input:
        job_to_clean    : name of the job(s) to clean (Wild cards are supported)
                            The name of the job(s) whose current logs are to be archived.
                            Specifying % will match ALL jobs.
        days_to_retain  : number of days to retain the logs for the specified job(s)
                            If 90 days are retained then all the archived logs that are older than 90 days are
                            purged/deleted. If 0 days are retained then all the archived logs are purged/deleted.

        Returns:
        return_code     : Output Return Code:
                            • S - Success.
                            • E - Error.
                            • F - Fatal/Unexpected Error.
        return_message  : Output message indicating the action applied or the reason for no action.
        result_number:  : Output Result number:
                            •  1 Success
                            • -2 Error : Error. e.g. Due to invalid job name or job not running
                            • -3 Fatal/Unexpected Error

        """
        # if options is "TRUNCATE" then all the archived logs are truncated.
        options = ""
        function_parameter_list = [days_to_retain, job_to_clean, options]

        sql = """
        DECLARE @out nvarchar(max),@out1 nvarchar(max),@out2 nvarchar(max);
        EXEC Ws_Job_Clear_Archive
          @p_sequence  = ?
        , @p_job_name  = ?
        , @p_task_name  = ?
        , @p_job_id = ?
        , @p_task_id = ?
        , @p_day_count = ?
        , @p_job = ?
        , @p_options = ?
        , @p_return_code = @out OUTPUT
        , @p_return_msg = @out1 OUTPUT
        , @p_result   = @out2 OUTPUT;
        SELECT @out AS return_code,@out1 AS return_msg,@out2 AS return_result;"""

        return_values = self.query_meta(
            sql, self.common_input_parameter_list + function_parameter_list
        )
        return_code = return_values[0][0]
        return_message = return_values[0][1]
        result_number = int(return_values[0][2])
        return return_code, return_message, result_number

    def update_task_log(
        self,
        inserted=0,
        updated=0,
        replaced=0,
        deleted=0,
        discarded=0,
        rejected=0,
        errored=0,
    ):
        """
        Updates row counts for a task in the Task Log using the WsWrkTask API from WhereScape.

        Updates row counts for the specified task in the Task Log. Task Log messages (and row counts)
        are accessible via the "Scheduler" tab/window and/or the WS_ADMIN_V_TASK view of the WS_WRK_TASK_RUN and
        WS_WRK_TASK_LOG tables.

        This routine is intended to be executed by a task of a job since it requires a valid job, task,
        and job sequence number that are provided by a WhereScape RED Scheduler.

        Input:
         - various counters

        Output Result Number:
        •  0 Success
        • -1 Warning
        • -3 Error

        Examples:
        # 100 records inserted
        update_task_log (100)
        update_task_log (inserted = 100, updated = 0, deleted = 0)

        """
        sql = """
        SET NOCOUNT ON
        DECLARE @out nvarchar(max);
        EXEC @out=WsWrkTask
        @p_job_key = ?
        , @p_task_key = ?
        , @p_sequence = ?
        , @p_inserted = ?
        , @p_updated   = ?
        , @p_replaced  = ?
        , @p_deleted    = ?
        , @p_discarded  = ?
        , @p_rejected  = ?
        , @p_errored   = ?;
        SELECT @out AS return_value;
        """

        sequence = os.environ["WSL_SEQUENCE"]
        job_name = os.environ["WSL_JOB_NAME"]
        task_name = os.environ["WSL_TASK_NAME"]
        job_id = os.environ["WSL_JOB_KEY"]
        task_id = os.environ["WSL_TASK_KEY"]

        parameters = [
            job_id,
            task_id,
            sequence,
            inserted,
            updated,
            replaced,
            deleted,
            discarded,
            rejected,
            errored,
        ]

        try:
            result = self.push_to_meta(sql, parameters)
            return result
        except Exception as e:
            self.error_messages.append(
                f"Error in update task log for job id/name: {job_id}  {job_name} task is/name: {task_id} {task_name} : {str(e)}"
            )
            self.error_messages.append(get_stack_trace_str())
            return None
