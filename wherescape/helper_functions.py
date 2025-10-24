from dateutil.parser import parse
from slugify import slugify


def create_column_names(display_names=[]):
    """
    Some api sources (like Notion) don't have column names that easily
    translate to a column name. This function slugifies the display names
    to column names. Every column receives the column number for uniqueness.
    Columns get truncated to 59 characters, because 63 characters is the max
    column lenght for Postgres columns.
    """
    i = 0
    columns = []
    for display_name in display_names:
        column = slugify(display_name, separator="_", max_length=59)
        if column == "":
            column = "column"
            column = f"{column}_{str(i + 1).zfill(3)}"
        if column in columns:
            column = f"{column}_{str(i + 1).zfill(3)}"
        columns.append(column)
        i += 1
    return columns


def create_legacy_column_names(display_names=[]):
    """
    LEGACY FUNCTION - Use create_column_names() for new tables instead.

    This was the original create_column_names() function that appended a
    zero-padded number suffix to EVERY column (e.g., column_name_001,
    column_name_002). While this guaranteed uniqueness, it created less
    readable column names than necessary.

    The function was replaced with a newer version that only adds numeric
    suffixes when needed (for duplicate or empty column names), resulting
    in cleaner column names in most cases.

    This legacy version is preserved because existing WhereScape tables may
    still use the old numbered column name format. Removing this function
    could break compatibility with those tables.

    Args:
        display_names (list): List of display names to convert to column names

    Returns:
        list: Column names with numbered suffixes (e.g., ['name_001', 'email_002'])
    """
    i = 0
    columns = []
    for display_name in display_names:
        column = slugify(display_name, separator="_", max_length=59)
        if column == "":
            column = "column"
        column = f"{column}_{str(i + 1).zfill(3)}"
        columns.append(column)
        i += 1
    return columns


def create_display_names(columns=[]):
    """
    Change column names in to display names.
    """
    display_names = []
    for column in columns:
        display_names.append(column.replace("_", " ").capitalize())
    return display_names


def prepare_metadata_query(
    lt_obj_key,
    src_table_name,
    columns=[],
    display_names=[],
    types=[],
    comments=[],
    source_columns=[],
):
    """
    Creating the metadata query to create the columns of an API load table is
    a lot of repetition. This function creates this query while making a few
    assumptions regarding nulls, numeric, additive and attribute. All provided
    lists should be the same size.
    """
    lt_obj_key = str(lt_obj_key)
    meta_values = ""
    i = 0

    for column in columns:
        if types[i] in ("text", "date", "timestamp", "bool", "varchar(256)"):
            nulls = "Y"
            numeric = "N"
            additive = "N"
            if types[i] in ("text", "bool"):
                attribute = "Y"
            else:
                attribute = "N"
        elif types[i] in ("numeric", "int", "bigint"):
            nulls = "Y"
            numeric = "Y"
            additive = "N"
            attribute = "N"
        else:
            nulls = "Y"
            numeric = "N"
            additive = "N"
            attribute = "N"

        if display_names == []:
            display_name = ""
        else:
            display_name = display_names[i]

        if source_columns == []:
            if display_names == []:
                source_column = ""
            else:
                source_column = display_names[i]
        else:
            source_column = source_columns[i]

        if comments == []:
            comment = display_name
        else:
            comment = comments[i]

        order = str((i + 1) * 10)
        meta_values += f"({lt_obj_key}, '{column}', '{display_name}', '{src_table_name}', '{source_column}', '{comment}', '{types[i]}', '{nulls}', '{numeric}', '{additive}', '{attribute}', '{order}')"
        i += 1
        if i != len(columns):
            meta_values += ",\n"
        else:
            meta_values += f",\n({lt_obj_key}, 'dss_record_source', 'dss record source', null, null, 'Record source.', 'varchar(256)', 'Y', 'N', 'N', 'Y', 99999991)"
            meta_values += f",\n({lt_obj_key}, 'dss_load_date', 'dss load date', null, null, 'Load date.', 'timestamp', 'Y', 'N', 'N', 'Y', 99999992)"

    # Create the actual sql
    sql = f"""
    DELETE FROM ws_load_col where lc_obj_key = {lt_obj_key};
    INSERT INTO dbo.ws_load_col (lc_obj_key, lc_col_name, lc_display_name, lc_src_table, lc_src_column, lc_src_strategy, lc_data_type, lc_nulls_flag, lc_numeric_flag, lc_additive_flag, lc_attribute_flag, lc_order)
    VALUES {meta_values};
    """

    return sql


def filter_dict(dict_to_filter, keys_to_keep):
    """
    This functions filters out keys that are unnecessary to keep.
    E.g. web url links to images on issues

    Parameters:
    dict_to_filter (object): The dict with all the keys and values still in it
    keys_to_keep (dict array): A view object with a list of the keys from a dict

    Returns:
    dict: The dict with only the key, value pairs you want to keep.
    """
    return dict([(key, dict_to_filter[key]) for key in dict_to_filter if key in set(keys_to_keep)])


def flatten_json(json_response, name_to_skip=None):
    """
    This function flattens the json_response from an API request.
    Nested dicts are flattened.

    Parameters:
    json_response (object): The dict that needs to be flattened
    name_to_skip (string): key to skip while flattening

    Returns:
    out: The dict with the flattened key value pairs
    """
    out = {}

    def flatten(x, name=""):
        if isinstance(x, dict):
            for a in x:
                if name_to_skip and name_to_skip == a:
                    new_name = name
                else:
                    new_name = name + a + "_"
                flatten(x[a], new_name)
        elif isinstance(x, list) and len(x) > 0:
            i = 0
            for a in x:
                flatten(a, name + str(i) + "_")
                i += 1
        else:
            out[name[:-1]] = x

    flatten(json_response)
    return out


def fill_out_empty_keys(cleaned_json, keys_to_keep, overwrite):
    """
    This function fills out empty keys for empty dicts returned by the API.

    Parameters:
        - cleaned_json (object): Dict with the flattened and cleaned json response
        - keys_to_keep (dict array): A view object with a list of the keys from a dict
        - overwrite (dict): A dictionary with a key, value pair to overwrite the none value with a fixed value

    Returns:
    out: The dict with all keys, value is None when there was nothing returned from the API
    """
    out = {}
    for key in keys_to_keep:
        if key not in set(cleaned_json.keys()):
            if overwrite and key in set(overwrite.keys()):
                out[key] = overwrite[key]
            else:
                out[key] = None
        else:
            out[key] = cleaned_json[key]
    return out


def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    string: str, string to check for date
    fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False
    except OverflowError:
        return False
