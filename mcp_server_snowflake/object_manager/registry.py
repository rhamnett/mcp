"""
Core registry for Snowflake object configurations.

This module defines the configuration for all simple Snowflake objects
that can be managed through the CoreObjectManager.
"""

from typing import Any, Dict

from snowflake.core.database import Database
from snowflake.core.database_role import DatabaseRole
from snowflake.core.role import Role
from snowflake.core.schema import Schema
from snowflake.core.warehouse import Warehouse

from mcp_server_snowflake.object_manager.managers.core import CoreObjectManager
from mcp_server_snowflake.object_manager.managers.function import FunctionManager
from mcp_server_snowflake.object_manager.managers.procedure import ProcedureManager
from mcp_server_snowflake.object_manager.managers.table import TableManager
from mcp_server_snowflake.object_manager.managers.view import ViewManager

# System databases that should never be dropped
SYSTEM_DATABASES = ["SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA", "INFORMATION_SCHEMA"]

# Core object registry with configurations for each object type
CORE_REGISTRY: Dict[str, Dict[str, Any]] = {
    "database": {
        "collection_path": "databases",
        "create_params": [
            "comment",
            "transient",
            "data_retention_time_in_days",
            "replace_if_exists",
        ],
        "parent_required": False,
        "manager_class": CoreObjectManager,
        "snowflake_class": Database,
        "validation": {
            "max_transient_retention_days": 1,
            "max_permanent_retention_days": 90,
            "protected_names": SYSTEM_DATABASES,
        },
    },
    "schema": {
        "collection_path": "databases[{database}].schemas",
        "create_params": [
            "comment",
            "transient",
            "data_retention_time_in_days",
            "replace_if_exists",
        ],
        "parent_required": True,
        "parent_params": ["database"],
        "manager_class": CoreObjectManager,
        "snowflake_class": Schema,
        "validation": {
            "max_transient_retention_days": 1,
            "max_permanent_retention_days": 90,
        },
    },
    "warehouse": {
        "collection_path": "warehouses",
        "create_params": [
            "warehouse_size",
            "warehouse_type",
            "auto_suspend",
            "auto_resume",
            "initially_suspended",
            "comment",
            "enable_query_acceleration",
            "query_acceleration_max_scale_factor",
            "max_cluster_count",
            "min_cluster_count",
            "scaling_policy",
        ],
        "parent_required": False,
        "manager_class": CoreObjectManager,
        "snowflake_class": Warehouse,
        "validation": {
            "warehouse_sizes": [
                "XSMALL",
                "SMALL",
                "MEDIUM",
                "LARGE",
                "XLARGE",
                "2XLARGE",
                "3XLARGE",
                "4XLARGE",
                "5XLARGE",
                "6XLARGE",
            ],
            "warehouse_types": ["STANDARD", "SNOWPARK-OPTIMIZED"],
            "scaling_policies": ["STANDARD", "ECONOMY"],
            "min_auto_suspend": 60,  # seconds
            "max_auto_suspend": 3600,  # seconds
            "min_clusters": 1,
            "max_clusters": 10,
        },
    },
    "role": {
        "collection_path": "roles",
        "create_params": ["comment"],
        "parent_required": False,
        "manager_class": CoreObjectManager,
        "snowflake_class": Role,
        "validation": {
            "protected_names": ["ACCOUNTADMIN", "SECURITYADMIN", "SYSADMIN", "PUBLIC"]
        },
    },
    "database_role": {
        "collection_path": "databases[{database}].database_roles",
        "create_params": ["comment"],
        "parent_required": True,
        "parent_params": ["database"],
        "manager_class": CoreObjectManager,
        "snowflake_class": DatabaseRole,
        "validation": {},
    },
    "table": {
        "collection_path": "databases[{database}].schemas[{schema}].tables",
        "create_params": ["columns", "comment"],
        "parent_required": True,
        "parent_params": ["database", "schema"],
        "complex_type": True,  # Requires custom manager
        "manager_class": TableManager,
        "snowflake_class": None,  # Custom managers don't use snowflake.core classes
        "validation": {"min_columns": 1, "required_column_fields": ["name", "type"]},
    },
    "view": {
        "collection_path": "databases[{database}].schemas[{schema}].views",
        "create_params": ["sql_text", "comment", "secure"],
        "parent_required": True,
        "parent_params": ["database", "schema"],
        "complex_type": True,  # Requires custom manager
        "manager_class": ViewManager,
        "snowflake_class": None,  # Custom managers don't use snowflake.core classes
        "validation": {"sql_must_start_with": "SELECT"},
    },
    "function": {
        "collection_path": "databases[{database}].schemas[{schema}].functions",
        "create_params": ["arguments", "returns", "language", "body", "comment"],
        "parent_required": True,
        "parent_params": ["database", "schema"],
        "complex_type": True,  # Requires custom manager
        "manager_class": FunctionManager,
        "snowflake_class": None,  # Custom managers don't use snowflake.core classes
        "validation": {
            "supported_languages": ["SQL", "JAVASCRIPT", "PYTHON", "JAVA", "SCALA"]
        },
    },
    "procedure": {
        "collection_path": "databases[{database}].schemas[{schema}].procedures",
        "create_params": ["arguments", "returns", "language", "body", "comment"],
        "parent_required": True,
        "parent_params": ["database", "schema"],
        "complex_type": True,  # Requires custom manager
        "manager_class": ProcedureManager,
        "snowflake_class": None,  # Custom managers don't use snowflake.core classes
        "validation": {
            "supported_languages": ["SQL", "JAVASCRIPT", "PYTHON", "JAVA", "SCALA"],
            "argument_modes": ["IN", "OUT", "INOUT"],
        },
    },
}


def get_object_config(object_type: str) -> Dict[str, Any]:
    """
    Get configuration for a specific object type.

    Parameters:
        object_type: The type of object (e.g., 'database', 'schema', 'warehouse')

    Returns:
        Configuration dictionary for the object type

    Raises:
        ValueError: If object type is not registered
    """
    if object_type not in CORE_REGISTRY:
        raise ValueError(
            f"Unknown object type: {object_type}. "
            f"Available types: {', '.join(CORE_REGISTRY.keys())}"
        )
    return CORE_REGISTRY[object_type]


def get_validation_config(object_type: str) -> Dict[str, Any]:
    """
    Get validation configuration for a specific object type.

    Parameters:
        object_type: The type of object (e.g., 'database', 'schema', 'warehouse')

    Returns:
        Validation configuration dictionary
    """
    config = get_object_config(object_type)
    return config.get("validation", {})


def is_system_object(object_type: str, name: str) -> bool:
    """
    Check if an object is a system/protected object.

    Parameters:
        object_type: The type of object
        name: The name of the object

    Returns:
        True if the object is protected, False otherwise
    """
    validation = get_validation_config(object_type)
    protected_names = validation.get("protected_names", [])
    return name.upper() in [p.upper() for p in protected_names]


def get_parent_params(object_type: str) -> list:
    """
    Get required parent parameters for an object type.

    Parameters:
        object_type: The type of object

    Returns:
        List of required parent parameter names
    """
    config = get_object_config(object_type)
    return config.get("parent_params", [])


def create_manager(object_type: str, snowflake_service: Any) -> Any:
    """Factory function to create manager instances."""
    config = CORE_REGISTRY[object_type]
    manager_class = config["manager_class"]
    snowflake_class = config.get("snowflake_class")

    return manager_class(
        snowflake_service=snowflake_service,
        object_type=object_type,
        object_class=snowflake_class,
        collection_path=config.get("collection_path"),
    )
