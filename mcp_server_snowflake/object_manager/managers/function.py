"""Function manager with custom create operation for UDF definitions."""

from typing import Any, Dict, List, Optional
from snowflake.core.function import Function, FunctionArgument
from .core import CoreObjectManager


class FunctionManager(CoreObjectManager):
    """Specialized manager for Snowflake user-defined functions."""
    
    async def create(self, **kwargs) -> Dict[str, Any]:
        """Create a user-defined function.
        
        Args:
            name: Function name
            database: Database name
            schema: Schema name
            arguments: List of argument definitions
                [{"name": "arg1", "type": "VARCHAR"}, ...]
            returns: Return type (e.g., "VARCHAR", "NUMBER")
            language: Function language (SQL, JAVASCRIPT, PYTHON, etc.)
            body: Function body/implementation
            comment: Optional function comment
            
        Returns:
            Dict with created function info
        """
        # Extract parameters
        name = kwargs.get('name')
        database = kwargs.get('database')
        schema = kwargs.get('schema')
        arguments_data = kwargs.get('arguments', [])
        returns = kwargs.get('returns')
        language = kwargs.get('language', 'SQL')
        body = kwargs.get('body')
        comment = kwargs.get('comment')
        
        if not all([name, database, schema, returns, body]):
            raise ValueError("name, database, schema, returns, and body are required")
        
        # Build argument objects
        arguments = []
        for arg_data in arguments_data:
            arg_name = arg_data.get('name')
            arg_type = arg_data.get('type')
            
            if not arg_name or not arg_type:
                raise ValueError(f"Argument must have 'name' and 'type': {arg_data}")
            
            # Map common SQL types to allowed enum values
            # Allowed: 'FIXED','INT','REAL','NUMBER','TEXT','BOOLEAN','DATE','TIME',
            #          'TIMESTAMP_TZ','TIMESTAMP_LTZ','TIMESTAMP_NTZ'
            datatype_map = {
                'VARCHAR': 'TEXT',
                'STRING': 'TEXT',
                'CHAR': 'TEXT',
                'INTEGER': 'INT',
                'BIGINT': 'INT',
                'SMALLINT': 'INT',
                'FLOAT': 'REAL',
                'DOUBLE': 'REAL',
                'DECIMAL': 'NUMBER',
                'NUMERIC': 'NUMBER',
                'TIMESTAMP': 'TIMESTAMP_TZ',
                'DATETIME': 'TIMESTAMP_TZ',
                'BOOL': 'BOOLEAN'
            }
            
            # Convert type to uppercase and map if needed
            upper_type = arg_type.upper()
            mapped_type = datatype_map.get(upper_type, upper_type)
            
            # Create FunctionArgument
            argument = FunctionArgument(
                name=arg_name,
                datatype=mapped_type
            )
            arguments.append(argument)
        
        # Map return type
        return_type_map = {
            'VARCHAR': 'TEXT',
            'STRING': 'TEXT',
            'CHAR': 'TEXT',
            'INTEGER': 'INT',
            'BIGINT': 'INT',
            'SMALLINT': 'INT',
            'FLOAT': 'REAL',
            'DOUBLE': 'REAL',
            'DECIMAL': 'NUMBER',
            'NUMERIC': 'NUMBER',
            'TIMESTAMP': 'TIMESTAMP_TZ',
            'DATETIME': 'TIMESTAMP_TZ',
            'BOOL': 'BOOLEAN'
        }
        
        upper_returns = returns.upper() if returns else 'TEXT'
        mapped_returns = return_type_map.get(upper_returns, upper_returns)
        
        # Create function object
        function = Function(
            name=name,
            arguments=arguments,
            returns=mapped_returns,
            language=language.upper(),
            body=body,
            comment=comment
        )
        
        # Get collection and create
        root = self._get_root()
        collection = root.databases[database].schemas[schema].functions
        collection.create(function)
        
        return {
            "object_type": "function",
            "name": name,
            "database": database,
            "schema": schema,
            "arguments": [
                {"name": arg.name, "type": str(arg.datatype)}
                for arg in arguments
            ],
            "returns": returns,
            "language": language,
            "comment": comment,
            "status": "created"
        }