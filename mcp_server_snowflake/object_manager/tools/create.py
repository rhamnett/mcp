"""
Create object tool for Snowflake MCP server.

This module provides the create_object dynamic tool that can handle
creation of any Snowflake object type.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Union

from mcp_server_snowflake.object_manager.registry import (
    CORE_REGISTRY,
    create_manager,
    get_object_config,
    get_parent_params,
    get_validation_config,
)
from mcp_server_snowflake.utils import SnowflakeException

logger = logging.getLogger(__name__)


async def create_object(
    snowflake_service: Any,
    object_type: str,
    name: str,
    # Parent parameters (optional, used by schemas, database_roles, etc.)
    database: Optional[str] = None,
    schema: Optional[str] = None,
    # Common creation parameters
    comment: Optional[str] = None,
    transient: Optional[bool] = False,
    data_retention_time_in_days: Optional[int] = None,
    replace_if_exists: Optional[bool] = False,
    # Warehouse-specific parameters
    warehouse_size: Optional[str] = None,
    warehouse_type: Optional[str] = None,
    auto_suspend: Optional[int] = None,
    auto_resume: Optional[bool] = None,
    initially_suspended: Optional[bool] = None,
    enable_query_acceleration: Optional[bool] = None,
    query_acceleration_max_scale_factor: Optional[int] = None,
    max_cluster_count: Optional[int] = None,
    min_cluster_count: Optional[int] = None,
    scaling_policy: Optional[str] = None,
    # Complex type parameters (tables, views, functions, procedures)
    columns: Optional[Union[str, List[Dict[str, Any]]]] = None,
    sql_text: Optional[str] = None,
    secure: Optional[bool] = False,
    arguments: Optional[Union[str, List[Dict[str, Any]]]] = None,
    returns: Optional[str] = None,
    language: Optional[str] = None,
    body: Optional[str] = None,
    clustering_keys: Optional[Union[str, List[str]]] = None,
) -> Dict[str, Any]:
    """
    Create a new Snowflake object of any supported type.

    This dynamic tool creates any type of Snowflake object based on the object_type
    parameter. It validates parameters according to the object type's configuration
    and calls the appropriate manager.

    Args:
        snowflake_service: Snowflake service instance
        object_type: Type of object to create (database, schema, warehouse, role,
                    database_role, table, view, function, procedure)
        name: Name of the object to create
        database: Database name (required for schemas, database_roles, and schema objects)
        schema: Schema name (required for tables, views, functions, procedures)
        comment: Optional comment for the object
        transient: Whether to create a transient object
        data_retention_time_in_days: Data retention period in days
        replace_if_exists: Whether to replace if object already exists
        warehouse_size: Size of warehouse (XSMALL, SMALL, MEDIUM, etc.)
        warehouse_type: Type of warehouse (STANDARD, SNOWPARK-OPTIMIZED)
        auto_suspend: Auto-suspend timeout in seconds
        auto_resume: Whether to auto-resume warehouse
        initially_suspended: Whether to create warehouse in suspended state
        enable_query_acceleration: Enable query acceleration for warehouse
        query_acceleration_max_scale_factor: Max scale factor for query acceleration
        max_cluster_count: Maximum number of clusters for multi-cluster warehouse
        min_cluster_count: Minimum number of clusters for multi-cluster warehouse
        scaling_policy: Scaling policy for multi-cluster warehouse
        columns: For tables - JSON string or list of column definitions
                [{"name": "col1", "type": "VARCHAR(50)", "nullable": true}, ...]
        sql_text: For views - SQL SELECT statement
        secure: For views - Whether to create a secure view
        arguments: For functions/procedures - JSON string or list of arguments
                  [{"name": "arg1", "type": "VARCHAR", "mode": "IN"}, ...]
        returns: For functions/procedures - Return type
        language: For functions/procedures - Implementation language
        body: For functions/procedures - Implementation body

    Returns:
        Dictionary with operation status and created object details

    Raises:
        SnowflakeException: If creation fails or parameters are invalid
    """
    try:
        # Validate object type
        if object_type not in CORE_REGISTRY:
            raise SnowflakeException(
                tool="create_object",
                message=f"Unknown object type: {object_type}. "
                f"Available types: {', '.join(CORE_REGISTRY.keys())}",
            )

        # Create manager directly from registry
        manager = create_manager(object_type, snowflake_service)

        # Get configuration for this object type
        config = get_object_config(object_type)
        validation = get_validation_config(object_type)

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
                        tool="create_object",
                        message=f"'{param}' is required for creating {object_type}",
                    )
                parent_params[param] = value

            # Handle optional schema parameter
            if schema and "schema" not in required_parents:
                parent_params["schema"] = schema

        # Build creation parameters based on what's allowed for this object type
        create_params = {}
        allowed_params = config.get("create_params", [])

        # Parse JSON strings for complex parameters
        parsed_columns = columns
        if isinstance(columns, str):
            try:
                parsed_columns = json.loads(columns)
            except json.JSONDecodeError:
                raise SnowflakeException(
                    tool="create_object",
                    message="'columns' must be a valid JSON array",
                )

        parsed_arguments = arguments
        if isinstance(arguments, str):
            try:
                parsed_arguments = json.loads(arguments)
            except json.JSONDecodeError:
                raise SnowflakeException(
                    tool="create_object",
                    message="'arguments' must be a valid JSON array",
                )

        parsed_clustering_keys = clustering_keys
        if isinstance(clustering_keys, str):
            try:
                parsed_clustering_keys = json.loads(clustering_keys)
            except json.JSONDecodeError:
                raise SnowflakeException(
                    tool="create_object",
                    message="'clustering_keys' must be a valid JSON array",
                )

        # Map each allowed parameter to its value
        param_mapping = {
            "comment": comment,
            "transient": transient,
            "data_retention_time_in_days": data_retention_time_in_days,
            "replace_if_exists": replace_if_exists,
            "warehouse_size": warehouse_size,
            "warehouse_type": warehouse_type,
            "auto_suspend": auto_suspend,
            "auto_resume": auto_resume,
            "initially_suspended": initially_suspended,
            "enable_query_acceleration": enable_query_acceleration,
            "query_acceleration_max_scale_factor": query_acceleration_max_scale_factor,
            "max_cluster_count": max_cluster_count,
            "min_cluster_count": min_cluster_count,
            "scaling_policy": scaling_policy,
            # Complex type parameters
            "columns": parsed_columns,
            "sql_text": sql_text,
            "secure": secure,
            "arguments": parsed_arguments,
            "returns": returns,
            "language": language,
            "body": body,
            "clustering_keys": parsed_clustering_keys,
        }

        for param in allowed_params:
            value = param_mapping.get(param)
            if value is not None:
                create_params[param] = value

        # Perform validation based on object type
        if object_type in ["database", "schema"]:
            # Validate retention days for transient vs permanent objects
            if transient and data_retention_time_in_days is not None:
                max_retention = validation.get("max_transient_retention_days", 1)
                if data_retention_time_in_days > max_retention:
                    raise SnowflakeException(
                        tool="create_object",
                        message=f"Transient {object_type}s can have maximum {max_retention} day retention",
                    )
            elif not transient and data_retention_time_in_days is not None:
                max_retention = validation.get("max_permanent_retention_days", 90)
                if data_retention_time_in_days > max_retention:
                    raise SnowflakeException(
                        tool="create_object",
                        message=f"Permanent {object_type}s can have maximum {max_retention} days retention",
                    )

        elif object_type == "warehouse":
            # Validate warehouse-specific parameters
            if warehouse_size:
                valid_sizes = validation.get("warehouse_sizes", [])
                if valid_sizes and warehouse_size.upper() not in valid_sizes:
                    raise SnowflakeException(
                        tool="create_object",
                        message=f"Invalid warehouse size. Must be one of: {', '.join(valid_sizes)}",
                    )
                create_params["warehouse_size"] = warehouse_size.upper()

            if warehouse_type:
                valid_types = validation.get("warehouse_types", [])
                if valid_types and warehouse_type.upper() not in valid_types:
                    raise SnowflakeException(
                        tool="create_object",
                        message=f"Invalid warehouse type. Must be one of: {', '.join(valid_types)}",
                    )
                create_params["warehouse_type"] = warehouse_type.upper()

            if auto_suspend is not None:
                min_suspend = validation.get("min_auto_suspend", 60)
                max_suspend = validation.get("max_auto_suspend", 3600)
                if auto_suspend < min_suspend or auto_suspend > max_suspend:
                    raise SnowflakeException(
                        tool="create_object",
                        message=f"auto_suspend must be between {min_suspend} and {max_suspend} seconds",
                    )

            if scaling_policy:
                valid_policies = validation.get("scaling_policies", [])
                if valid_policies and scaling_policy.upper() not in valid_policies:
                    raise SnowflakeException(
                        tool="create_object",
                        message=f"Invalid scaling policy. Must be one of: {', '.join(valid_policies)}",
                    )
                create_params["scaling_policy"] = scaling_policy.upper()

        logger.info(f"Creating {object_type}: {name} with params: {create_params}")

        # Check if this is a complex type that needs special handling
        if config.get("complex_type", False):
            # For complex types, pass all params directly to the custom manager
            all_params = {"name": name, **parent_params, **create_params}
            result = await manager.create(**all_params)
        else:
            # For simple types, use the standard CoreObjectManager interface
            result = await manager.create(
                name=name,
                parent_params=parent_params if parent_params else None,
                **create_params,
            )

        return result

    except SnowflakeException:
        raise
    except Exception as e:
        error_msg = f"Failed to create {object_type}: {str(e)}"
        logger.error(error_msg)
        raise SnowflakeException(tool="create_object", message=error_msg)


def create_object_wrapper(snowflake_service: Any):
    """
    Create a wrapper for create_object that pre-binds the snowflake_service parameter.

    Args:
        snowflake_service: The Snowflake service instance to bind

    Returns:
        Wrapped function with snowflake_service parameter pre-bound
    """

    async def wrapper(
        object_type: str,
        name: str,
        # Parent parameters (optional, used by schemas, database_roles, etc.)
        database: Optional[str] = None,
        schema: Optional[str] = None,
        # Common creation parameters
        comment: Optional[str] = None,
        transient: Optional[bool] = False,
        data_retention_time_in_days: Optional[int] = None,
        replace_if_exists: Optional[bool] = False,
        # Warehouse-specific parameters
        warehouse_size: Optional[str] = None,
        warehouse_type: Optional[str] = None,
        auto_suspend: Optional[int] = None,
        auto_resume: Optional[bool] = None,
        initially_suspended: Optional[bool] = None,
        enable_query_acceleration: Optional[bool] = None,
        query_acceleration_max_scale_factor: Optional[int] = None,
        max_cluster_count: Optional[int] = None,
        min_cluster_count: Optional[int] = None,
        scaling_policy: Optional[str] = None,
        # Complex type parameters (tables, views, functions, procedures)
        columns: Optional[Union[str, List[Dict[str, Any]]]] = None,
        sql_text: Optional[str] = None,
        secure: Optional[bool] = False,
        arguments: Optional[Union[str, List[Dict[str, Any]]]] = None,
        returns: Optional[str] = None,
        language: Optional[str] = None,
        body: Optional[str] = None,
        clustering_keys: Optional[Union[str, List[str]]] = None,
    ) -> Dict[str, Any]:
        """
        Create a new Snowflake object of any supported type.

        See create_object function for full parameter documentation.
        """
        return await create_object(
            snowflake_service=snowflake_service,
            object_type=object_type,
            name=name,
            database=database,
            schema=schema,
            comment=comment,
            transient=transient,
            data_retention_time_in_days=data_retention_time_in_days,
            replace_if_exists=replace_if_exists,
            warehouse_size=warehouse_size,
            warehouse_type=warehouse_type,
            auto_suspend=auto_suspend,
            auto_resume=auto_resume,
            initially_suspended=initially_suspended,
            enable_query_acceleration=enable_query_acceleration,
            query_acceleration_max_scale_factor=query_acceleration_max_scale_factor,
            max_cluster_count=max_cluster_count,
            min_cluster_count=min_cluster_count,
            scaling_policy=scaling_policy,
            columns=columns,
            sql_text=sql_text,
            secure=secure,
            arguments=arguments,
            returns=returns,
            language=language,
            body=body,
            clustering_keys=clustering_keys,
        )

    return wrapper
