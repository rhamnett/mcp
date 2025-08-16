"""
List objects tool for Snowflake MCP server.

This module provides the list_objects dynamic tool that can handle
listing of any Snowflake object type.
"""

import logging
from typing import Any, Dict, List, Optional

from mcp_server_snowflake.object_manager.registry import (
    CORE_REGISTRY,
    create_manager,
    get_object_config,
    get_parent_params,
)
from mcp_server_snowflake.utils import SnowflakeException

logger = logging.getLogger(__name__)


async def list_objects(
    snowflake_service: Any,
    object_type: str,
    like: Optional[str] = None,
    starts_with: Optional[str] = None,
    limit: int = 100,
    # Parent parameters (optional, used by schemas, database_roles, etc.)
    database: Optional[str] = None,
    schema: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    List Snowflake objects of any supported type.

    This dynamic tool lists any type of Snowflake object based on the object_type
    parameter. It supports filtering by pattern or prefix.

    Args:
        snowflake_service: Snowflake service instance
        object_type: Type of objects to list (database, schema, warehouse, role, database_role)
        like: SQL LIKE pattern for filtering (e.g., 'TEST_%')
        starts_with: Filter objects starting with this prefix
        limit: Maximum number of results (1-10000)
        database: Database name (required for schemas and database_roles)
        schema: Schema name (optional for some objects)

    Returns:
        List of dictionaries containing object details

    Raises:
        SnowflakeException: If listing fails or parameters are invalid
    """
    try:
        # Validate object type
        if object_type not in CORE_REGISTRY:
            raise SnowflakeException(
                tool="list_objects",
                message=f"Unknown object type: {object_type}. "
                f"Available types: {', '.join(CORE_REGISTRY.keys())}",
            )

        # Create manager for this object type

        manager = create_manager(object_type, snowflake_service)

        # Validate mutually exclusive parameters
        if like and starts_with:
            raise SnowflakeException(
                tool="list_objects",
                message="Cannot use both 'like' and 'starts_with' parameters",
            )

        # Validate limit
        if not 1 <= limit <= 10000:
            raise SnowflakeException(
                tool="list_objects",
                message="Limit must be between 1 and 10000",
            )

        # Get configuration for this object type
        config = get_object_config(object_type)

        # Build parent parameters if required
        parent_params = {}
        if config.get("parent_required", False):
            required_parents = get_parent_params(object_type)
            for param in required_parents:
                # Get the actual parameter value
                if param == "database":
                    value = database
                elif param == "schema":
                    value = schema
                else:
                    value = None

                if not value:
                    raise SnowflakeException(
                        tool="list_objects",
                        message=f"'{param}' is required for listing {object_type}s",
                    )
                parent_params[param] = value

            # Handle optional schema parameter
            if schema and "schema" not in required_parents:
                parent_params["schema"] = schema

        logger.info(
            f"Listing {object_type}s with filters: like={like}, starts_with={starts_with}, "
            f"limit={limit}, parent={parent_params}"
        )

        # Call the manager's list method
        result = await manager.list(
            like=like,
            starts_with=starts_with,
            limit=limit,
            parent_params=parent_params if parent_params else None,
        )

        return result

    except SnowflakeException:
        raise
    except Exception as e:
        error_msg = f"Failed to list {object_type}s: {str(e)}"
        logger.error(error_msg)
        raise SnowflakeException(tool="list_objects", message=error_msg)


def list_objects_wrapper(snowflake_service: Any):
    """
    Create a wrapper for list_objects that pre-binds the snowflake_service parameter.

    Args:
        snowflake_service: The Snowflake service instance to bind

    Returns:
        Wrapped function with snowflake_service parameter pre-bound
    """

    async def wrapper(
        object_type: str,
        like: Optional[str] = None,
        starts_with: Optional[str] = None,
        limit: int = 100,
        # Parent parameters
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        List Snowflake objects of any supported type.

        See list_objects function for full parameter documentation.
        """
        return await list_objects(
            snowflake_service=snowflake_service,
            object_type=object_type,
            like=like,
            starts_with=starts_with,
            limit=limit,
            database=database,
            schema=schema,
        )

    return wrapper
