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
Database management functionality for Snowflake MCP server.

This module provides the core DatabaseManager class that handles all
database-related operations using the Snowflake Core SDK.
"""

import logging
from typing import Any, Dict, List, Optional

from ..utils import SnowflakeException

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Manages database operations for the Snowflake MCP server.
    
    This class provides methods for creating, listing, and dropping databases
    using the Snowflake Core SDK. It integrates with the existing SnowflakeService
    and uses its OAuth authentication connection.
    
    Attributes
    ----------
    service : SnowflakeService
        The parent Snowflake service providing connection and authentication
    _root : Optional[snowflake.core.Root]
        Cached Snowflake Core Root object for database operations
    """
    
    def __init__(self, snowflake_service):
        """
        Initialize the DatabaseManager with a SnowflakeService instance.
        
        Parameters
        ----------
        snowflake_service : SnowflakeService
            The parent service providing connection and authentication
        """
        self.service = snowflake_service
        self._root = None
        logger.info("DatabaseManager initialized")
    
    def _get_root(self):
        """
        Get or create the Snowflake Core Root object.
        
        Lazy initialization of the Root object using the existing
        OAuth connection from the SnowflakeService.
        
        Returns
        -------
        snowflake.core.Root
            The Root object for accessing Snowflake Core SDK functionality
        
        Raises
        ------
        SnowflakeException
            If Root object creation fails
        """
        if self._root is None:
            try:
                from snowflake.core import Root
                
                # Use the existing connection from SnowflakeService
                self._root = Root(self.service.connection)
                logger.debug("Snowflake Core Root object created successfully")
            except ImportError as e:
                logger.error("snowflake-core package not installed")
                raise SnowflakeException(
                    tool="DatabaseManager",
                    message="snowflake-core package is required for database operations",
                    status_code=500
                )
            except Exception as e:
                logger.error(f"Failed to create Root object: {str(e)}")
                raise SnowflakeException(
                    tool="DatabaseManager",
                    message=f"Failed to initialize database manager: {str(e)}",
                    status_code=500
                )
        
        return self._root
    
    async def create_database(self, name: str, comment: Optional[str] = None,
                             transient: bool = False, replace_if_exists: bool = False,
                             data_retention_time_in_days: Optional[int] = None) -> Dict[str, Any]:
        """
        Create a new Snowflake database.
        
        Creates a database with the specified parameters, supporting both
        permanent and transient databases with configurable retention times.
        
        Parameters
        ----------
        name : str
            Name of the database to create
        comment : Optional[str]
            Optional comment/description for the database
        transient : bool
            Whether to create a transient database (default: False)
        replace_if_exists : bool
            Whether to replace if database exists (default: False)
        data_retention_time_in_days : Optional[int]
            Data retention time in days
        
        Returns
        -------
        dict
            Operation result containing database details:
            {
                "status": "success",
                "database": {
                    "name": "DB_NAME",
                    "kind": "PERMANENT" or "TRANSIENT",
                    "comment": "Description",
                    "created_on": "2024-01-01T00:00:00Z",
                    "data_retention_time_in_days": 1
                }
            }
        
        Raises
        ------
        SnowflakeException
            If database creation fails
        """
        logger.info(f"Creating database: {name}")
        
        try:
            from snowflake.core import CreateMode
            from snowflake.core.database import Database
            
            root = self._get_root()
            
            # Build database parameters
            db_params = {"name": name}
            
            if comment:
                db_params["comment"] = comment
            
            # Set database kind (PERMANENT or TRANSIENT)
            db_params["kind"] = "TRANSIENT" if transient else "PERMANENT"
            
            # Set retention time if specified
            if data_retention_time_in_days is not None:
                db_params["data_retention_time_in_days"] = data_retention_time_in_days
            
            logger.debug(f"Creating Database object with parameters: {db_params}")
            database = Database(**db_params)
            
            # Determine create mode
            mode = CreateMode.or_replace if replace_if_exists else CreateMode.error_if_exists
            
            # Create the database
            logger.info(f"Executing database creation for: {name}")
            db_ref = root.databases.create(database, mode=mode)
            
            # Fetch the created database details
            created_db = db_ref.fetch()
            
            result = {
                "status": "success",
                "database": {
                    "name": created_db.name,
                    "kind": created_db.kind,
                    "comment": created_db.comment,
                    "created_on": str(created_db.created_on) if hasattr(created_db, 'created_on') else None,
                    "data_retention_time_in_days": created_db.data_retention_time_in_days,
                }
            }
            
            logger.info(f"Database '{name}' created successfully")
            return result
            
        except Exception as e:
            logger.error(f"Failed to create database '{name}': {str(e)}")
            raise SnowflakeException(
                tool="create_database",
                message=f"Database creation failed: {str(e)}",
                status_code=500
            )
    
    async def list_databases(self, like: Optional[str] = None,
                            starts_with: Optional[str] = None,
                            limit: int = 10000) -> Dict[str, Any]:
        """
        List Snowflake databases with optional filtering.
        
        Retrieves database metadata with support for LIKE patterns and
        prefix filtering for efficient querying.
        
        Parameters
        ----------
        like : Optional[str]
            SQL LIKE pattern for filtering database names
        starts_with : Optional[str]
            Filter databases whose names start with this prefix
        limit : int
            Maximum number of databases to return (default: 10000)
        
        Returns
        -------
        dict
            List of databases and metadata:
            {
                "status": "success",
                "count": 10,
                "databases": [
                    {
                        "name": "DB_NAME",
                        "kind": "PERMANENT",
                        "comment": "Description",
                        "created_on": "2024-01-01T00:00:00Z"
                    },
                    ...
                ]
            }
        
        Raises
        ------
        SnowflakeException
            If database listing fails
        """
        logger.info(f"Listing databases with filters: like={like}, "
                   f"starts_with={starts_with}, limit={limit}")
        
        try:
            root = self._get_root()
            
            # Build iterator parameters
            iter_params = {"limit": limit}
            
            if like:
                iter_params["like"] = like
                logger.debug(f"Applying LIKE filter: {like}")
            
            if starts_with:
                iter_params["starts_with"] = starts_with
                logger.debug(f"Applying starts_with filter: {starts_with}")
            
            # Iterate through databases
            databases = []
            count = 0
            
            for db in root.databases.iter(**iter_params):
                count += 1
                logger.debug(f"Found database {count}: {db.name} ({db.kind})")
                
                databases.append({
                    "name": db.name,
                    "kind": db.kind,
                    "comment": db.comment,
                    "created_on": str(db.created_on) if hasattr(db, 'created_on') else None,
                })
            
            logger.info(f"Successfully listed {len(databases)} databases")
            
            return {
                "status": "success",
                "count": len(databases),
                "databases": databases
            }
            
        except Exception as e:
            logger.error(f"Failed to list databases: {str(e)}")
            raise SnowflakeException(
                tool="list_databases",
                message=f"Database listing failed: {str(e)}",
                status_code=500
            )
    
    async def drop_database(self, name: str, cascade: bool = False,
                           if_exists: bool = True) -> Dict[str, Any]:
        """
        Drop a Snowflake database.
        
        Permanently removes a database and all its contents. Supports
        conditional dropping with IF EXISTS clause for safer operations.
        
        Parameters
        ----------
        name : str
            Name of the database to drop
        cascade : bool
            Whether to drop with CASCADE (default: False)
        if_exists : bool
            Whether to use IF EXISTS clause (default: True)
        
        Returns
        -------
        dict
            Operation status:
            {
                "status": "success",
                "message": "Database 'DB_NAME' dropped"
            }
        
        Raises
        ------
        SnowflakeException
            If database drop operation fails
        """
        logger.info(f"Dropping database: {name}")
        
        try:
            root = self._get_root()
            
            # Drop the database
            if cascade:
                # Note: The Snowflake Core SDK's drop method doesn't directly support CASCADE
                # We'll need to execute raw SQL for CASCADE option
                with self.service.get_connection() as (con, cur):
                    cascade_clause = " CASCADE" if cascade else ""
                    if_exists_clause = "IF EXISTS " if if_exists else ""
                    sql = f"DROP DATABASE {if_exists_clause}{name}{cascade_clause}"
                    
                    logger.debug(f"Executing SQL: {sql}")
                    cur.execute(sql)
            else:
                # Use the Core SDK for standard drop
                root.databases[name].drop(if_exists=if_exists)
            
            logger.info(f"Database '{name}' dropped successfully")
            
            return {
                "status": "success",
                "message": f"Database '{name}' dropped"
            }
            
        except Exception as e:
            logger.error(f"Failed to drop database '{name}': {str(e)}")
            
            # Provide more specific error message for common cases
            error_msg = str(e)
            if "does not exist" in error_msg.lower():
                raise SnowflakeException(
                    tool="drop_database",
                    message=f"Database '{name}' does not exist",
                    status_code=404
                )
            elif "insufficient privileges" in error_msg.lower():
                raise SnowflakeException(
                    tool="drop_database",
                    message=f"Insufficient privileges to drop database '{name}'",
                    status_code=403
                )
            else:
                raise SnowflakeException(
                    tool="drop_database",
                    message=f"Database drop failed: {str(e)}",
                    status_code=500
                )