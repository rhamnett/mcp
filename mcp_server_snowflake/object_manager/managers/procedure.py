"""Procedure manager with custom create operation for stored procedure definitions."""

from typing import Any, Dict

from snowflake.core.procedure import (
    Argument,
    JavaFunction,
    JavaScriptFunction,
    Procedure,
    PythonFunction,
    ReturnDataType,
    ScalaFunction,
    SQLFunction,
)

from mcp_server_snowflake.object_manager.managers.core import CoreObjectManager


class ProcedureManager(CoreObjectManager):
    """Specialized manager for Snowflake stored procedures."""

    async def create(self, **kwargs) -> Dict[str, Any]:
        """Create a stored procedure.

        Args:
            name: Procedure name
            database: Database name
            schema: Schema name
            arguments: List of argument definitions
                [{"name": "arg1", "type": "VARCHAR", "mode": "IN"}, ...]
            returns: Return type (e.g., "VARCHAR", "TABLE(...)")
            language: Procedure language (SQL, JAVASCRIPT, PYTHON, etc.)
            body: Procedure body/implementation
            comment: Optional procedure comment

        Returns:
            Dict with created procedure info
        """
        # Extract parameters
        name = kwargs.get("name")
        database = kwargs.get("database")
        schema = kwargs.get("schema")
        arguments_data = kwargs.get("arguments", [])
        returns = kwargs.get("returns")
        language = kwargs.get("language", "SQL")
        body = kwargs.get("body")
        comment = kwargs.get("comment")

        if not all([name, database, schema, returns, body]):
            raise ValueError("name, database, schema, returns, and body are required")

        # Build argument objects
        arguments = []
        for arg_data in arguments_data:
            arg_name = arg_data.get("name")
            arg_type = arg_data.get("type")
            # Note: mode is not supported in the current API, arguments are always IN

            if not arg_name or not arg_type:
                raise ValueError(f"Argument must have 'name' and 'type': {arg_data}")

            # Map common SQL types to allowed enum values
            datatype_map = {
                "VARCHAR": "TEXT",
                "STRING": "TEXT",
                "CHAR": "TEXT",
                "INTEGER": "INT",
                "BIGINT": "INT",
                "SMALLINT": "INT",
                "FLOAT": "REAL",
                "DOUBLE": "REAL",
                "DECIMAL": "NUMBER",
                "NUMERIC": "NUMBER",
                "TIMESTAMP": "TIMESTAMP_TZ",
                "DATETIME": "TIMESTAMP_TZ",
                "BOOL": "BOOLEAN",
            }

            upper_type = arg_type.upper()
            mapped_type = datatype_map.get(upper_type, upper_type)

            # Create Argument
            argument = Argument(name=arg_name, datatype=mapped_type)
            arguments.append(argument)

        # Create return type object
        return_type_map = {
            "VARCHAR": "TEXT",
            "STRING": "TEXT",
            "CHAR": "TEXT",
            "INTEGER": "INT",
            "BIGINT": "INT",
            "SMALLINT": "INT",
            "FLOAT": "REAL",
            "DOUBLE": "REAL",
            "DECIMAL": "NUMBER",
            "NUMERIC": "NUMBER",
            "TIMESTAMP": "TIMESTAMP_TZ",
            "DATETIME": "TIMESTAMP_TZ",
            "BOOL": "BOOLEAN",
        }

        upper_returns = returns.upper() if returns else "TEXT"
        mapped_returns = return_type_map.get(upper_returns, upper_returns)
        return_type = ReturnDataType(datatype=mapped_returns)

        # Create language-specific config
        lang_upper = language.upper()
        if lang_upper == "SQL":
            language_config = SQLFunction()
        elif lang_upper == "JAVASCRIPT":
            language_config = JavaScriptFunction()
        elif lang_upper == "PYTHON":
            language_config = PythonFunction()
        elif lang_upper == "JAVA":
            language_config = JavaFunction()
        elif lang_upper == "SCALA":
            language_config = ScalaFunction()
        else:
            # Default to SQL
            language_config = SQLFunction()

        # Create procedure object
        procedure = Procedure(
            name=name,
            arguments=arguments,
            return_type=return_type,
            language_config=language_config,
            body=body,
            comment=comment,
        )

        # Get collection and create
        root = self._get_root()
        collection = root.databases[database].schemas[schema].procedures
        collection.create(procedure)

        return {
            "object_type": "procedure",
            "name": name,
            "database": database,
            "schema": schema,
            "arguments": [
                {"name": arg.name, "type": str(arg.datatype)} for arg in arguments
            ],
            "returns": returns,
            "language": language,
            "comment": comment,
            "status": "created",
        }
