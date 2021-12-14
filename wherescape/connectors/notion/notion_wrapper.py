from datetime import datetime
import logging

from notion_client import Client, APIResponseError


class Notion:
    """
    Notion API wrapper. Uses https://github.com/ramnes/notion-sdk-py. Has
    functions for Notion databases. Main purpuse is to get the data from
    Notion databases to move to a load table.
    """

    def __init__(self, key):
        """
        Uses key to authenticate the Client. Tries an API call to test the
        connection.
        """
        self.client = Client(auth=key)
        try:
            # This call is just to check the validity of the API key
            self.client.databases.list()
        except APIResponseError as e:
            logging.error(e)
            raise

    def get_notion_database(self, database_id):
        """
        Requests a database structure including possible select values from
        Notion. Does not give the data from the database. Returns the title
        and the column data.
        """
        try:
            database = self.client.databases.retrieve(database_id)
        except Exception as e:
            logging.error(e)
            raise

        if len(database["title"]) > 0:
            title = database["title"][0]["text"]["content"]
        else:
            title = "<untitled>"
        database_data = self.client.databases.query(database_id=database_id)
        return title, database_data

    def get_notion_database_columns(self, database):
        """
        Get the column names and the translated Postgress column type. Only
        translatable columns will be used. Not all field types are implemented
        yet. Unimplemented fields will raise a NotImplementedError.
        """
        if len(database["results"]) != 0:
            types = []
            columns = []
            for title, field in database["results"][0]["properties"].items():
                if field["type"] in (
                    "select",
                    "title",
                    "rich_text",
                    "multi_select",
                ):
                    columns.append(title)
                    types.append("text")
                if field["type"] in (
                    "url",
                    "email",
                    "phone",
                ):
                    # Not implemented, probably text
                    raise NotImplementedError
                elif field["type"] == "number":
                    # Not implemented, probably numeric
                    raise NotImplementedError
                elif field["type"] in ("date"):
                    columns.append(title)
                    types.append("date")
                elif field["type"] in ("created_time"):
                    # Not implemented, should be date
                    raise NotImplementedError
                elif field["type"] == "checkbox":
                    # Not implemented, should be boolean
                    raise NotImplementedError
                elif field["type"] in ("people", "created_by", "last_edited_by"):
                    # Not implemented, probably text
                    raise NotImplementedError
                elif field["type"] == "file":
                    # Not implemented
                    raise NotImplementedError
                elif field["type"] == "relation":
                    # Not implemented
                    raise NotImplementedError
                elif field["type"] == "formula":
                    # Not implemented, probably text
                    raise NotImplementedError
            return columns, types

    def get_notion_database_data(self, database):
        """
        Get the data from the supplied Notion database.
        """
        columns, _ = self.get_notion_database_columns(database)
        values = []
        for notion_row in database["results"]:
            row = []
            for title in columns:
                field = notion_row["properties"][title]
                if field["type"] == "select":
                    value = field["select"]["name"]
                elif field["type"] == "rich_text":
                    if len(field["rich_text"]):
                        value = field["rich_text"][0]["plain_text"]
                    else:
                        value = ""
                elif field["type"] == "date":
                    if field["date"] and field["date"]["start"]:
                        value = datetime.strptime(field["date"]["start"], "%Y-%m-%d")
                    else:
                        value = None
                elif field["type"] == "multi_select":
                    if len(field["multi_select"]):
                        value = ",".join(item["name"] for item in field["multi_select"])
                    else:
                        value = ""
                elif field["type"] == "title":
                    if len(field["title"]):
                        value = field["title"][0]["plain_text"]
                    else:
                        value = ""
                row.append(value)
            values.append(row)
        return values

    def list_databases(self):
        """
        List all Notion databases that have been shared with the authenticated
        integration. So databases need to be be shared before appearing in
        this list.
        """
        result = []
        databases = self.client.databases.list()
        if len(databases["results"]) > 0:
            for database in databases["results"]:
                logging.info(database["title"][0]["plain_text"])
                result.append((database["id"], database["title"][0]["plain_text"]))
        return result
