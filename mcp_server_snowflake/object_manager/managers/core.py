"""
Core object manager for Snowflake MCP server.

This module provides the base manager class for managing Snowflake objects
through the snowflake.core API. It handles common operations like create,
list, and drop for simple objects that follow standard patterns.
"""

import logging
from typing import Any, Dict, List, Optional, Type

from snowflake.core import CreateMode, Root
from snowflake.core.exceptions import NotFoundError

from mcp_server_snowflake.utils import SnowflakeException

logger = logging.getLogger(__name__)


class CoreObjectManager:
    """
    Base manager for simple Snowflake objects.

    This class provides a unified interface for managing Snowflake objects
    that follow standard patterns. It handles connection management, error
    handling, and common operations through the snowflake.core API.

    Attributes:
        service: The SnowflakeService instance for connection management
        object_type: The type of object being managed (e.g., 'database', 'schema')
        object_class: The snowflake.core class for this object type
        collection_path: The path to the collection in the Root object
    """

    def __init__(
        self,
        snowflake_service: Any,
        object_type: str,
        object_class: Type,
        collection_path: Optional[str] = None,
    ):
        """
        Initialize the CoreObjectManager.

        Args:
            snowflake_service: The SnowflakeService instance
            object_type: The type of object (e.g., 'database', 'warehouse')
            object_class: The snowflake.core class for this object type
            collection_path: The path to the collection (defaults to f"{object_type}s")
        """
        self.service = snowflake_service
        self.object_type = object_type
        self.object_class = object_class
        self.collection_path = collection_path or f"{object_type}s"

        logger.debug(
            f"Initialized CoreObjectManager for {object_type} "
            f"with collection path: {self.collection_path}"
        )

    def _get_root(self) -> Root:
        """
        Get the Root object from snowflake.core.

        Returns:
            The Root object connected to the current session

        Raises:
            SnowflakeException: If connection is not available
        """
        if not self.service.connection:
            raise SnowflakeException(
                tool=self.object_type,
                message=f"No active Snowflake connection for {self.object_type} operations",
                status_code=500,
            )

        return Root(self.service.connection)

    def _get_collection(self, parent_params: Optional[Dict[str, str]] = None):
        """
        Get the collection for this object type.

        Handles both simple paths (e.g., 'databases') and parent-based paths
        (e.g., 'databases[{database}].schemas').

        Args:
            parent_params: Parameters for parent-based paths (e.g., {'database': 'MY_DB'})

        Returns:
            The collection object from snowflake.core

        Raises:
            SnowflakeException: If collection cannot be accessed
        """
        root = self._get_root()

        # Handle parent-based paths like 'databases[{database}].schemas'
        if "[" in self.collection_path and parent_params:
            path_parts = self.collection_path.split(".")
            current = root

            for part in path_parts:
                if "[" in part:
                    # Extract collection name and parameter
                    collection_name = part.split("[")[0]
                    param_name = part.split("{")[1].split("}")[0]

                    if param_name not in parent_params:
                        raise SnowflakeException(
                            tool=self.object_type,
                            message=f"Missing required parent parameter: {param_name}",
                            status_code=400,
                        )

                    # Navigate to parent collection and get specific item
                    collection = getattr(current, collection_name)
                    current = collection[parent_params[param_name]]
                else:
                    # Simple navigation to next level
                    current = getattr(current, part)

            return current
        else:
            # Simple path navigation
            path_parts = self.collection_path.split(".")
            current = root

            for part in path_parts:
                current = getattr(current, part)

            return current

    async def create(
        self, name: str, parent_params: Optional[Dict[str, str]] = None, **kwargs
    ) -> Dict[str, Any]:
        """
        Create a new object.

        Args:
            name: The name of the object to create
            parent_params: Parameters for parent-based objects (e.g., {'database': 'MY_DB'})
            **kwargs: Additional parameters for object creation

        Returns:
            A dictionary with creation details

        Raises:
            SnowflakeException: If creation fails
        """
        try:
            logger.info(f"Creating {self.object_type}: {name}")

            # Get the collection
            collection = self._get_collection(parent_params)

            # Build the object with provided parameters
            object_params = {"name": name}

            # Handle transient objects for databases and schemas
            # The Database and Schema classes use 'kind' parameter instead of 'transient'
            if "transient" in kwargs:
                transient_value = kwargs.pop("transient")  # Remove it immediately
                if transient_value:
                    if self.object_type in ["database", "schema"]:
                        object_params["kind"] = "TRANSIENT"
                    else:
                        # For other object types that might support transient directly
                        object_params["transient"] = True
                elif self.object_type in ["database", "schema"]:
                    # Explicitly set PERMANENT for non-transient databases/schemas
                    object_params["kind"] = "PERMANENT"

            # Extract create mode parameter before adding to object_params
            replace_if_exists = kwargs.pop("replace_if_exists", False)

            # Special handling for warehouse boolean parameters (Snowflake expects lowercase strings)
            if self.object_type == "warehouse":
                warehouse_bool_params = [
                    "auto_resume",
                    "initially_suspended",
                    "enable_query_acceleration",
                ]
                for param in warehouse_bool_params:
                    if param in kwargs and kwargs[param] is not None:
                        # Convert boolean to lowercase string
                        object_params[param] = str(kwargs.pop(param)).lower()

            # Add all other parameters (transient and replace_if_exists are already removed)
            for key, value in kwargs.items():
                if value is not None:
                    object_params[key] = value

            # Create the object instance
            obj = self.object_class(**object_params)

            # Determine create mode based on replace_if_exists
            if replace_if_exists:
                mode = CreateMode.or_replace
            else:
                mode = CreateMode.if_not_exists

            # Create the object in Snowflake
            collection.create(obj, mode=mode)

            logger.info(f"Successfully created {self.object_type}: {name}")

            return {
                "success": True,
                "object_type": self.object_type,
                "name": name,
                "message": f"{self.object_type.capitalize()} '{name}' created successfully",
            }

        except Exception as e:
            error_msg = f"Failed to create {self.object_type} '{name}': {str(e)}"
            logger.error(error_msg)

            if "already exists" in str(e).lower():
                raise SnowflakeException(
                    tool=self.object_type, message=error_msg, status_code=409
                )
            elif "insufficient privileges" in str(e).lower():
                raise SnowflakeException(
                    tool=self.object_type, message=error_msg, status_code=403
                )
            else:
                raise SnowflakeException(
                    tool=self.object_type, message=error_msg, status_code=500
                )

    async def list(
        self,
        like: Optional[str] = None,
        starts_with: Optional[str] = None,
        limit: int = 100,
        parent_params: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        List objects with optional filtering.

        Args:
            like: SQL LIKE pattern for filtering (e.g., 'TEST_%')
            starts_with: Prefix filter (alternative to LIKE)
            limit: Maximum number of results to return (default: 100)
            parent_params: Parameters for parent-based objects

        Returns:
            A list of dictionaries containing object information

        Raises:
            SnowflakeException: If listing fails
        """
        try:
            logger.info(
                f"Listing {self.object_type}s with filters: like={like}, starts_with={starts_with}, limit={limit}"
            )

            # Validate mutually exclusive parameters
            if like and starts_with:
                raise SnowflakeException(
                    tool=self.object_type,
                    message="Cannot use both 'like' and 'starts_with' parameters",
                    status_code=400,
                )

            # Validate limit
            if not 1 <= limit <= 10000:
                raise SnowflakeException(
                    tool=self.object_type,
                    message="Limit must be between 1 and 10000",
                    status_code=400,
                )

            # Get the collection
            collection = self._get_collection(parent_params)

            # Build the filter pattern
            pattern = None
            if like:
                pattern = like
            elif starts_with:
                pattern = f"{starts_with}%"

            # Iterate through objects
            objects = []
            count = 0

            for obj in collection.iter(like=pattern):
                if count >= limit:
                    break

                # Extract object information
                obj_info = {"name": obj.name, "object_type": self.object_type}

                # Add additional attributes if available
                if hasattr(obj, "comment") and obj.comment:
                    obj_info["comment"] = obj.comment

                if hasattr(obj, "created_on"):
                    obj_info["created_on"] = str(obj.created_on)

                if hasattr(obj, "owner"):
                    obj_info["owner"] = obj.owner

                # Add transient flag if applicable
                # For databases and schemas, check the 'kind' attribute
                if self.object_type in ["database", "schema"] and hasattr(obj, "kind"):
                    obj_info["transient"] = obj.kind == "TRANSIENT"
                elif hasattr(obj, "transient"):
                    obj_info["transient"] = obj.transient

                objects.append(obj_info)
                count += 1

            logger.info(f"Found {len(objects)} {self.object_type}(s)")
            return objects

        except Exception as e:
            error_msg = f"Failed to list {self.object_type}s: {str(e)}"
            logger.error(error_msg)

            if "insufficient privileges" in str(e).lower():
                raise SnowflakeException(
                    tool=self.object_type, message=error_msg, status_code=403
                )
            else:
                raise SnowflakeException(
                    tool=self.object_type, message=error_msg, status_code=500
                )

    async def drop(
        self,
        name: str,
        if_exists: bool = True,
        cascade: bool = False,
        parent_params: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Drop an object.

        Args:
            name: The name of the object to drop
            if_exists: If True, don't error if object doesn't exist
            cascade: If True, drop dependent objects as well
            parent_params: Parameters for parent-based objects

        Returns:
            A dictionary with drop operation details

        Raises:
            SnowflakeException: If drop operation fails
        """
        try:
            logger.info(
                f"Dropping {self.object_type}: {name} (if_exists={if_exists}, cascade={cascade})"
            )

            # Check for system objects (if applicable)
            if self.object_type == "database":
                system_databases = [
                    "SNOWFLAKE",
                    "SNOWFLAKE_SAMPLE_DATA",
                    "INFORMATION_SCHEMA",
                ]
                if name.upper() in system_databases:
                    raise SnowflakeException(
                        tool=self.object_type,
                        message=f"Cannot drop system database: {name}",
                        status_code=403,
                    )

            # Get the collection
            collection = self._get_collection(parent_params)

            try:
                # Get the object reference
                obj_ref = collection[name]

                # Drop the object
                if cascade:
                    obj_ref.drop(if_exists=if_exists)
                else:
                    obj_ref.drop(if_exists=if_exists)

                logger.info(f"Successfully dropped {self.object_type}: {name}")

                return {
                    "success": True,
                    "object_type": self.object_type,
                    "name": name,
                    "message": f"{self.object_type.capitalize()} '{name}' dropped successfully",
                }

            except NotFoundError:
                if if_exists:
                    logger.info(
                        f"{self.object_type.capitalize()} '{name}' does not exist (if_exists=True)"
                    )
                    return {
                        "success": True,
                        "object_type": self.object_type,
                        "name": name,
                        "message": f"{self.object_type.capitalize()} '{name}' does not exist",
                    }
                else:
                    raise SnowflakeException(
                        tool=self.object_type,
                        message=f"{self.object_type.capitalize()} '{name}' not found",
                        status_code=404,
                    )

        except SnowflakeException:
            # Re-raise SnowflakeExceptions as-is
            raise
        except Exception as e:
            error_msg = f"Failed to drop {self.object_type} '{name}': {str(e)}"
            logger.error(error_msg)

            if "insufficient privileges" in str(e).lower():
                raise SnowflakeException(
                    tool=self.object_type, message=error_msg, status_code=403
                )
            elif "cannot be dropped" in str(e).lower():
                raise SnowflakeException(
                    tool=self.object_type, message=error_msg, status_code=409
                )
            else:
                raise SnowflakeException(
                    tool=self.object_type, message=error_msg, status_code=500
                )
