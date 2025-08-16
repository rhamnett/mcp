"""
Drop object tool for Snowflake MCP server.

This module provides the drop_object dynamic tool that can handle
dropping of any Snowflake object type.
"""

import logging
from typing import Any, Dict, Optional

from mcp_server_snowflake.object_manager.registry import (
    CORE_REGISTRY,
    create_manager,
    get_object_config,
    get_parent_params,
    is_system_object,
)
from mcp_server_snowflake.utils import SnowflakeException

logger = logging.getLogger(__name__)


async def drop_object(
    snowflake_service: Any,
    object_type: str,
    name: str,
    if_exists: bool = True,
    cascade: bool = False,
    # Parent parameters (optional, used by schemas, database_roles, etc.)
    database: Optional[str] = None,
    schema: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Drop a Snowflake object of any supported type.

    This dynamic tool drops any type of Snowflake object based on the object_type
    parameter. It includes protection for system objects.

    Args:
        snowflake_service: Snowflake service instance
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
            )

        # Create manager for this object type
        manager = create_manager(object_type, snowflake_service)

        # Validate name
        if not name:
            raise SnowflakeException(
                tool="drop_object",
                message=f"'name' is required for dropping {object_type}",
            )

        # Get configuration for this object type
        config = get_object_config(object_type)

        # Build parent parameters if they might be needed
        parent_params = {}
        if config.get("parent_required", False):
            parent_params_list = get_parent_params(object_type)
            for param in parent_params_list:
                # Get the actual parameter value
                if param == "database":
                    value = database
                elif param == "schema":
                    value = schema
                else:
                    value = None
                if value:
                    parent_params[param] = value

            # Handle optional schema parameter
            if schema and "schema" not in parent_params_list:
                parent_params["schema"] = schema

        # Special protection for system/protected objects
        if is_system_object(object_type, name):
            raise SnowflakeException(
                tool="drop_object",
                message=f"Cannot drop protected {object_type}: {name}",
            )

        logger.info(
            f"Dropping {object_type}: {name} (if_exists={if_exists}, cascade={cascade}, parent={parent_params})"
        )

        # Call the manager's drop method
        result = await manager.drop(
            name=name,
            if_exists=if_exists,
            cascade=cascade,
            parent_params=parent_params if parent_params else None,
        )

        return result

    except SnowflakeException:
        raise
    except Exception as e:
        error_msg = f"Failed to drop {object_type}: {str(e)}"
        logger.error(error_msg)
        raise SnowflakeException(tool="drop_object", message=error_msg)


def drop_object_wrapper(snowflake_service: Any):
    """
    Create a wrapper for drop_object that pre-binds the snowflake_service parameter.

    Args:
        snowflake_service: The Snowflake service instance to bind

    Returns:
        Wrapped function with snowflake_service parameter pre-bound
    """

    async def wrapper(
        object_type: str,
        name: str,
        if_exists: bool = True,
        cascade: bool = False,
        # Parent parameters
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Drop a Snowflake object of any supported type.

        See drop_object function for full parameter documentation.
        """
        return await drop_object(
            snowflake_service=snowflake_service,
            object_type=object_type,
            name=name,
            if_exists=if_exists,
            cascade=cascade,
            database=database,
            schema=schema,
        )

    return wrapper
