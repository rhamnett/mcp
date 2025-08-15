"""
Drop object tool for Snowflake MCP server.

This module provides the drop_object dynamic tool that can handle 
dropping of any Snowflake object type.
"""

import logging
from typing import Any, Dict, Optional
from mcp_server_snowflake.utils import SnowflakeException
from ..registry import (
    CORE_REGISTRY,
    get_object_config,
    get_parent_params,
    is_system_object
)

logger = logging.getLogger(__name__)


async def drop_object(
    manager_registry: Dict[str, Any],
    object_type: str,
    name: str,
    if_exists: bool = True,
    cascade: bool = False,
    # Parent parameters (optional, used by schemas, database_roles, etc.)
    database: Optional[str] = None,
    schema: Optional[str] = None
) -> Dict[str, Any]:
    """
    Drop a Snowflake object of any supported type.
    
    This dynamic tool drops any type of Snowflake object based on the object_type
    parameter. It includes protection for system objects.
    
    Args:
        manager_registry: Registry of object managers
        object_type: Type of object to drop (database, schema, warehouse, role, database_role)
        name: Name of the object to drop
        if_exists: Only drop if the object exists (avoids errors)
        cascade: Drop dependent objects as well
        database: Database name (optional, used for schemas and database_roles)
        schema: Schema name (optional for some objects)
        
    Returns:
        Dictionary with operation status
        
    Raises:
        SnowflakeException: If drop fails or parameters are invalid
    """
    try:
        # Validate object type
        if object_type not in CORE_REGISTRY:
            raise SnowflakeException(
                tool="drop_object",
                message=f"Unknown object type: {object_type}. "
                f"Available types: {', '.join(CORE_REGISTRY.keys())}",
                status_code=400
            )
        
        # Get manager for this object type
        if object_type not in manager_registry:
            raise SnowflakeException(
                tool="drop_object",
                message=f"No manager available for object type: {object_type}",
                status_code=500
            )
        
        manager = manager_registry[object_type]
        
        # Validate name
        if not name:
            raise SnowflakeException(
                tool="drop_object",
                message=f"'name' is required for dropping {object_type}",
                status_code=400
            )
        
        # Get configuration for this object type
        config = get_object_config(object_type)
        
        # Build parent parameters if they might be needed
        parent_params = {}
        if config.get('parent_required', False):
            parent_params_list = get_parent_params(object_type)
            for param in parent_params_list:
                # Get the actual parameter value
                if param == 'database':
                    value = database
                elif param == 'schema':
                    value = schema
                else:
                    value = None
                if value:
                    parent_params[param] = value
            
            # Handle optional schema parameter
            if schema and 'schema' not in parent_params_list:
                parent_params['schema'] = schema
        
        # Special protection for system/protected objects
        if is_system_object(object_type, name):
            raise SnowflakeException(
                tool="drop_object",
                message=f"Cannot drop protected {object_type}: {name}",
                status_code=403
            )
        
        logger.info(
            f"Dropping {object_type}: {name} (if_exists={if_exists}, cascade={cascade}, parent={parent_params})"
        )
        
        # Call the manager's drop method
        result = await manager.drop(
            name=name,
            if_exists=if_exists,
            cascade=cascade,
            parent_params=parent_params if parent_params else None
        )
        
        return result
        
    except SnowflakeException:
        raise
    except Exception as e:
        error_msg = f"Failed to drop {object_type}: {str(e)}"
        logger.error(error_msg)
        raise SnowflakeException(tool="drop_object", message=error_msg, status_code=500)