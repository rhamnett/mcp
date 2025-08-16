def get_create_object_prompt(object_types):
    return f"""List any type of Snowflake objects with optional filtering.
Supported types: {", ".join(object_types)}.
Specify the object_type parameter to choose what to list."""


def get_list_objects_prompt(object_types):
    return f"""List any type of Snowflake objects with optional filtering.
Supported types: {", ".join(object_types)}.
Specify the object_type parameter to choose what to list."""


def get_drop_object_prompt(object_types):
    return f"""Drop any type of Snowflake object.
Supported types: {", ".join(object_types)}.
Specify the object_type parameter to choose what to drop."""
