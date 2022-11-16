"""
Template ws_env.py file that writes environment variables.

Usage
------------------
1) Modify this script with actual usernames and passwords, such that they match
      what is normally set in the .bat file that WhereScape generates.
2) Save the file as ws_env.py (in your local development environment)
3) Make sure that ws_env.py is in .gitignore
"""
import os


def setup_env(tablename, schema="load", environment="dev1", source="dev1"):
    """
    Import and execute this function to set environment variables as if WhereScape was running.

    This way you can run python scripts independant from the WhereScape UI
    """
    capped_environment = environment.capitalize()

    os.environ["WSL_META_DSN"] = f"ws{capped_environment}Repo"
    os.environ["WSL_META_SERVER"] = "EC2AMAZ-XXXXX"
    os.environ["WSL_META_DSN_ARCH"] = "64"
    os.environ["WSL_META_DBID"] = ""

    os.environ["WSL_META_DB"] = f"ws{capped_environment}Repo"
    os.environ["WSL_META_SCHEMA"] = "dbo."
    os.environ["WSL_META_USER"] = f"{environment}RepoLogin"
    os.environ["WSL_META_PWD"] = "__pasword__"

    if schema is not None:
        os.environ["WSL_LOAD_FULLNAME"] = f"{schema}.{tablename}"
        os.environ["WSL_LOAD_TABLE"] = tablename
        os.environ["WSL_LOAD_SCHEMA"] = schema
        os.environ["WSL_LOAD_DB"] = "warehouse"

        os.environ["WSL_TGT_DSN"] = f"wswh {environment}"
        os.environ["WSL_TGT_DSN_ARCH"] = "64"
        os.environ["WSL_TGT_SERVER"] = "xxxxxxx.xxxxxxxx.eu-central-1.rds.amazonaws.com"
        os.environ["WSL_TGT_DBPORT"] = ""
        os.environ["WSL_TGT_DBID"] = ""
        # These were missing in .bat file
        #    os.environ['WSL_TGT_DB']="__db_name__"
        #    os.environ['WSL_TGT_SCHEMA']=""
        os.environ["WSL_TGT_USER"] = "__target_user__"
        os.environ["WSL_TGT_PWD"] = "__pasword__"

        if source == "sgmt":
            os.environ["WSL_SRC_DSN"] = "wswh prod"
            os.environ["WSL_SRC_DSN_ARCH"] = "64"
            os.environ[
                "WSL_SRC_SERVER"
            ] = "xxxxxxx.xxxxxxx.eu-central-1.rds.amazonaws.com"
            os.environ["WSL_SRC_DBPORT"] = ""
            os.environ["WSL_SRC_DBID"] = ""
            os.environ["WSL_SRC_DB"] = "__db_name__"
            os.environ["WSL_SRC_SCHEMA"] = "__schema_name__"
            os.environ["WSL_SRC_USER"] = "__user_name__"
            os.environ["WSL_SRC_PWD"] = "__pasword__"
        else:
            os.environ["WSL_SRC_DSN"] = "wswh dev1"
            os.environ["WSL_SRC_DSN_ARCH"] = "64"
            os.environ[
                "WSL_SRC_SERVER"
            ] = "xxxxxx.xxxxxx.eu-central-1.rds.amazonaws.com"
            os.environ["WSL_SRC_DBPORT"] = ""
            os.environ["WSL_SRC_DBID"] = ""
            os.environ["WSL_SRC_DB"] = "__db_name__"
            os.environ["WSL_SRC_SCHEMA"] = ""
            os.environ["WSL_SRC_USER"] = "__user_name__"
            os.environ["WSL_SRC_PWD"] = "__pasword__"
    else:
        # for directly called hosts scripts
        os.environ["WSL_LOAD_FULLNAME"] = ""
        os.environ["WSL_LOAD_TABLE"] = ""
        os.environ["WSL_LOAD_SCHEMA"] = ""
        os.environ["WSL_LOAD_DB"] = ""

        os.environ["WSL_TGT_DSN"] = ""
        os.environ["WSL_TGT_DSN_ARCH"] = "32"
        os.environ["WSL_TGT_SERVER"] = ""
        os.environ["WSL_TGT_DBPORT"] = ""
        os.environ["WSL_TGT_DBID"] = ""
        # These were missing in .bat file
        #    os.environ['WSL_TGT_DB']="warehouse"
        #    os.environ['WSL_TGT_SCHEMA']=""
        os.environ["WSL_TGT_USER"] = ""
        os.environ["WSL_TGT_PWD"] = ""

        os.environ["WSL_SRC_DSN"] = ""
        os.environ["WSL_SRC_DSN_ARCH"] = "32"
        os.environ["WSL_SRC_SERVER"] = ""
        os.environ["WSL_SRC_DBPORT"] = ""
        os.environ["WSL_SRC_DBID"] = ""
        os.environ["WSL_SRC_DB"] = ""
        os.environ["WSL_SRC_SCHEMA"] = ""
        os.environ["WSL_SRC_USER"] = ""
        os.environ["WSL_SRC_PWD"] = ""

    os.environ["WSL_BINDIR"] = "C:\\Program Files\\WhereScape\\RED\\"
    # os.environ['WSL_WORKDIR'] = f"C:\\WhereScape\\Scheduler\\Work\\{capped_environment}\\"
    os.environ[
        "WSL_WORKDIR"
    ] = "C:\\Users\\Bart\\Documents\\GitHub\\wherescape-warehouse\\"

    os.environ["WSL_SEQUENCE"] = "1234"
    os.environ["WSL_JOB_KEY"] = "9999"
    os.environ["WSL_JOB_NAME"] = "Local env"
    os.environ["WSL_TASK_KEY"] = "6666"
    os.environ["WSL_TASK_NAME"] = tablename

    os.environ["WSL_TEMP_DB"] = ""

    # setup passwords for warehouse.py
    git_dir = r"C:\WhereScape\GitlabRepository"

    os.environ["CONFIG_JSON_PATH"] = (
        git_dir + r"\wherescape-warehouse\python\config.wherescape.json"
    )

    # -- set passwords
    # -- script can run in all environments
    os.environ["WSWH_L_ETL"] = "__pasword__"
    os.environ["WSWH_D1_ETL"] = "__pasword__"
    os.environ["WSWH_D2_ETL"] = "__pasword__"
    os.environ["WSWH_A_ETL"] = "__pasword__"
    os.environ["WSWH_P_ETL"] = "__pasword__"


if __name__ == "__main__":
    setup_env("loadtablename", schema="load", environment="dev1")

    print(os.environ["WSL_LOAD_FULLNAME"])
