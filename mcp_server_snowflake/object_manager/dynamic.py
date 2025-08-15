"""
Dynamic tool orchestration for Snowflake MCP server.

This module provides the create_dynamic_tools function that creates wrapped
versions of the 3 dynamic tools with the manager registry bound.
"""

from typing import Any, Dict, List, Optional
from .tools.create import create_object
from .tools.list import list_objects
from .tools.drop import drop_object


def create_dynamic_tools(object_managers: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create the 3 dynamic tool functions with the manager registry bound.
    
    This function creates wrapped versions of the dynamic tools that have
    the manager registry pre-bound, making them suitable for registration
    with FastMCP.
    
    Args:
        object_managers: Dictionary mapping object types to their managers
        
    Returns:
        Dictionary with the 3 dynamic tool functions
    """
    
    async def create_object_wrapper(
        object_type: str,
        name: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        comment: Optional[str] = None,
        transient: bool = False,
        data_retention_time_in_days: Optional[int] = None,
        replace_if_exists: bool = False,
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
        # Schema object parameters
        columns: Optional[str] = None,  # JSON array for table columns
        sql_text: Optional[str] = None,  # SQL for views
        arguments: Optional[str] = None,  # JSON array for function/procedure arguments
        returns: Optional[str] = None,  # Return type for functions/procedures
        language: Optional[str] = None,  # Language for functions/procedures
        body: Optional[str] = None,  # Body for functions/procedures
        secure: bool = False,  # For secure views
        clustering_keys: Optional[str] = None  # JSON array for table clustering keys
    ) -> Dict[str, Any]:
        """Create a new Snowflake object of the specified type."""
        return await create_object(
            manager_registry=object_managers,
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
            arguments=arguments,
            returns=returns,
            language=language,
            body=body,
            secure=secure,
            clustering_keys=clustering_keys
        )
    
    async def list_objects_wrapper(
        object_type: str,
        like: Optional[str] = None,
        starts_with: Optional[str] = None,
        limit: int = 100,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List Snowflake objects of the specified type with optional filtering."""
        return await list_objects(
            manager_registry=object_managers,
            object_type=object_type,
            like=like,
            starts_with=starts_with,
            limit=limit,
            database=database,
            schema=schema
        )
    
    async def drop_object_wrapper(
        object_type: str,
        name: str,
        if_exists: bool = True,
        cascade: bool = False,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ) -> Dict[str, Any]:
        """Drop a Snowflake object of the specified type."""
        return await drop_object(
            manager_registry=object_managers,
            object_type=object_type,
            name=name,
            if_exists=if_exists,
            cascade=cascade,
            database=database,
            schema=schema
        )
    
    # Set proper function metadata
    create_object_wrapper.__name__ = "create_object"
    list_objects_wrapper.__name__ = "list_objects"
    drop_object_wrapper.__name__ = "drop_object"
    
    return {
        'create_object': create_object_wrapper,
        'list_objects': list_objects_wrapper,
        'drop_object': drop_object_wrapper
    }