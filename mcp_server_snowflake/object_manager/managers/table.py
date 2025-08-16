"""Table manager with custom create operation for column definitions."""

from typing import Any, Dict

from snowflake.core.table import Table, TableColumn

from mcp_server_snowflake.object_manager.managers.core import CoreObjectManager


class TableManager(CoreObjectManager):
    """Specialized manager for Snowflake tables with column support."""

    async def create(self, **kwargs) -> Dict[str, Any]:
        """Create a table with column definitions.

        Args:
            name: Table name
            database: Database name
            schema: Schema name
            columns: List of column definitions
                [{"name": "col1", "type": "VARCHAR(50)", "nullable": true}, ...]
            comment: Optional table comment

        Returns:
            Dict with created table info
        """
        # Extract parameters
        name = kwargs.get("name")
        database = kwargs.get("database")
        schema = kwargs.get("schema")
        columns_data = kwargs.get("columns", [])
        comment = kwargs.get("comment")

        if not all([name, database, schema, columns_data]):
            raise ValueError("name, database, schema, and columns are required")

        if not columns_data:
            raise ValueError("At least one column must be specified")

        # Build column objects
        columns = []
        for col_data in columns_data:
            col_name = col_data.get("name")
            col_type = col_data.get("type")
            nullable = col_data.get("nullable", True)

            if not col_name or not col_type:
                raise ValueError(f"Column must have 'name' and 'type': {col_data}")

            # Create TableColumn
            column = TableColumn(name=col_name, datatype=col_type, nullable=nullable)
            columns.append(column)

        # Create table object
        table = Table(name=name, columns=columns, comment=comment)

        # Get collection and create
        root = self._get_root()
        collection = root.databases[database].schemas[schema].tables
        collection.create(table)

        return {
            "object_type": "table",
            "name": name,
            "database": database,
            "schema": schema,
            "columns": [
                {"name": col.name, "type": str(col.datatype), "nullable": col.nullable}
                for col in columns
            ],
            "comment": comment,
            "status": "created",
        }
