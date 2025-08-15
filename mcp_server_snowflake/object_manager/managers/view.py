"""View manager with custom create operation for SQL text."""

from typing import Any, Dict, Optional
from snowflake.core.view import View, ViewColumn
from .core import CoreObjectManager


class ViewManager(CoreObjectManager):
    """Specialized manager for Snowflake views with SQL text support."""
    
    async def create(self, **kwargs) -> Dict[str, Any]:
        """Create a view with SQL definition.
        
        Args:
            name: View name
            database: Database name
            schema: Schema name
            sql_text: SQL SELECT statement for the view
            comment: Optional view comment
            secure: Whether to create a secure view (default: False)
            
        Returns:
            Dict with created view info
        """
        # Extract parameters
        name = kwargs.get('name')
        database = kwargs.get('database')
        schema = kwargs.get('schema')
        sql_text = kwargs.get('sql_text')
        comment = kwargs.get('comment')
        secure = kwargs.get('secure', False)
        
        if not all([name, database, schema, sql_text]):
            raise ValueError("name, database, schema, and sql_text are required")
        
        # Validate SQL text starts with SELECT
        sql_upper = sql_text.strip().upper()
        if not sql_upper.startswith('SELECT'):
            raise ValueError("View sql_text must be a SELECT statement")
        
        # Use raw SQL for view creation as the View object has issues with empty columns
        secure_clause = "SECURE " if secure else ""
        comment_clause = f" COMMENT = '{comment}'" if comment else ""
        
        create_sql = f"""
        CREATE OR REPLACE {secure_clause}VIEW {database}.{schema}.{name}
        {comment_clause}
        AS {sql_text}
        """
        
        # Execute raw SQL
        cursor = self.service.connection.cursor()
        cursor.execute(create_sql)
        cursor.close()
        
        return {
            "object_type": "view",
            "name": name,
            "database": database,
            "schema": schema,
            "sql_text": sql_text,
            "secure": secure,
            "comment": comment,
            "status": "created"
        }