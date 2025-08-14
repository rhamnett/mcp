# Copyright 2025 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Tool wrapper functions for database operations.

This module provides MCP tool wrappers for database management operations,
following the official server's pattern for tool registration and execution.
"""

import logging
from typing import Annotated, Dict, Optional

from ..utils import SnowflakeException

logger = logging.getLogger(__name__)


def create_database_tools(snowflake_service) -> Dict:
    """
    Create database tool wrapper functions for MCP registration.
    
    This function creates and returns wrapper functions for all database
    operations, following the pattern used for Cortex services in the
    official server.
    
    Parameters
    ----------
    snowflake_service : SnowflakeService
        The Snowflake service instance with database manager
    
    Returns
    -------
    dict
        Dictionary mapping tool names to wrapper functions:
        {
            "create_database": create_database_wrapper,
            "list_databases": list_databases_wrapper,
            "drop_database": drop_database_wrapper
        }
    """
    tools = {}
    
    # Get the database manager from the service
    db_manager = snowflake_service.get_database_manager()
    
    if db_manager is None:
        logger.warning("DatabaseManager not initialized, database tools will not be available")
        return tools
    
    # Create Database Tool
    async def create_database_wrapper(
        name: str,
        comment: Optional[str] = None,
        transient: bool = False,
        data_retention_time_in_days: Optional[int] = None,
        replace_if_exists: bool = False,
    ) -> Dict:
        """
        Create a new Snowflake database.
        
        Creates a permanent or transient database with configurable retention
        and optional replacement of existing databases.
        
        Parameters
        ----------
        name : str
            Database name (required)
        comment : str, optional
            Descriptive comment about the database
        transient : bool, optional
            Create transient database (default: False)
        data_retention_time_in_days : int, optional
            Time Travel retention period (0-90 days)
        replace_if_exists : bool, optional
            Replace if database exists (default: False)
        
        Returns
        -------
        dict
            Database creation status and details
        
        Raises
        ------
        SnowflakeException
            If creation fails
        """
        try:
            # Basic validation
            if not name or not name.strip():
                raise SnowflakeException(
                    tool="create_database",
                    message="Database name is required",
                    status_code=400
                )
            
            # Validate retention time if provided
            if data_retention_time_in_days is not None:
                if transient and data_retention_time_in_days > 1:
                    raise SnowflakeException(
                        tool="create_database",
                        message="Transient databases can only have 0-1 days retention",
                        status_code=400
                    )
                elif not transient and data_retention_time_in_days > 90:
                    raise SnowflakeException(
                        tool="create_database",
                        message="Permanent databases can only have 0-90 days retention",
                        status_code=400
                    )
            
            # Execute database creation
            result = await db_manager.create_database(
                name=name,
                comment=comment,
                transient=transient,
                data_retention_time_in_days=data_retention_time_in_days,
                replace_if_exists=replace_if_exists
            )
            return result
            
        except SnowflakeException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error in create_database: {str(e)}")
            raise SnowflakeException(
                tool="create_database",
                message=f"Database creation failed: {str(e)}",
                status_code=500
            )
    
    # List Databases Tool
    async def list_databases_wrapper(
        like: Optional[str] = None,
        starts_with: Optional[str] = None,
        limit: int = 100,
    ) -> Dict:
        """
        List Snowflake databases with optional filtering.
        
        Retrieves database metadata with support for pattern matching
        and prefix filtering.
        
        Parameters
        ----------
        like : str, optional
            SQL LIKE pattern for filtering
        starts_with : str, optional
            Prefix filter (cannot be used with 'like')
        limit : int, optional
            Maximum results to return (default: 100)
        
        Returns
        -------
        dict
            List of databases and count
        
        Raises
        ------
        SnowflakeException
            If listing fails
        """
        try:
            # Basic validation
            if like and starts_with:
                raise SnowflakeException(
                    tool="list_databases",
                    message="Cannot use both 'like' and 'starts_with' filters",
                    status_code=400
                )
            
            if limit < 1 or limit > 10000:
                raise SnowflakeException(
                    tool="list_databases",
                    message="Limit must be between 1 and 10000",
                    status_code=400
                )
            
            # Execute database listing
            result = await db_manager.list_databases(
                like=like,
                starts_with=starts_with,
                limit=limit
            )
            return result
            
        except SnowflakeException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error in list_databases: {str(e)}")
            raise SnowflakeException(
                tool="list_databases",
                message=f"Database listing failed: {str(e)}",
                status_code=500
            )
    
    # Drop Database Tool
    async def drop_database_wrapper(
        name: str,
        cascade: bool = False,
        if_exists: bool = True,
    ) -> Dict:
        """
        Drop a Snowflake database.
        
        Permanently removes a database and all its contents. This operation
        cannot be undone unless recovered with UNDROP within retention period.
        
        Parameters
        ----------
        name : str
            Database name to drop (required)
        cascade : bool, optional
            Drop with CASCADE option (default: False)
        if_exists : bool, optional
            Only drop if exists (default: True)
        
        Returns
        -------
        dict
            Drop operation status
        
        Raises
        ------
        SnowflakeException
            If drop operation fails
        """
        try:
            # Basic validation
            if not name or not name.strip():
                raise SnowflakeException(
                    tool="drop_database",
                    message="Database name is required",
                    status_code=400
                )
            
            # Protect system databases
            system_dbs = ['SNOWFLAKE', 'SNOWFLAKE_SAMPLE_DATA', 'INFORMATION_SCHEMA']
            if name.upper() in system_dbs:
                raise SnowflakeException(
                    tool="drop_database",
                    message=f"Cannot drop system database: {name}",
                    status_code=403
                )
            
            # Execute database drop
            result = await db_manager.drop_database(
                name=name,
                cascade=cascade,
                if_exists=if_exists
            )
            return result
            
        except SnowflakeException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error in drop_database: {str(e)}")
            raise SnowflakeException(
                tool="drop_database",
                message=f"Database drop failed: {str(e)}",
                status_code=500
            )
    
    # Register tools in dictionary
    tools['create_database'] = create_database_wrapper
    tools['list_databases'] = list_databases_wrapper
    tools['drop_database'] = drop_database_wrapper
    
    logger.info(f"Created {len(tools)} database tool wrappers")
    
    return tools


def get_database_tool_descriptions() -> Dict[str, str]:
    """
    Get descriptions for database tools.
    
    Returns
    -------
    dict
        Mapping of tool names to their descriptions for MCP registration
    """
    return {
        'create_database': (
            "Create a new Snowflake database with options for transient/permanent, "
            "retention time, and replacement of existing databases"
        ),
        'list_databases': (
            "List Snowflake databases with optional filtering by pattern or prefix"
        ),
        'drop_database': (
            "Drop a Snowflake database permanently (use with caution)"
        )
    }