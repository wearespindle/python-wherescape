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
    return dict(
        [
            (key, dict_to_filter[key])
            for key in dict_to_filter
            if key in set(keys_to_keep)
        ]
    )


def flatten_json(json_response, name_to_skip=None):
    out = {}

    def flatten(x, name=""):
        if type(x) is dict:
            for a in x:
                if name_to_skip and name_to_skip == a:
                    new_name = name
                else:
                    new_name = name + a + "_"
                flatten(x[a], new_name)
        elif type(x) is list and x:
            i = 0
            for a in x:
                flatten(a, name + str(i) + "_")
                i += 1
        else:
            out[name[:-1]] = x

    flatten(json_response)
    return out
