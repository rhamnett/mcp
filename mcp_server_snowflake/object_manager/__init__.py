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
Object management module for Snowflake MCP server.

This module provides database and other object management capabilities
using the Snowflake Core SDK.
"""

from .database_manager import DatabaseManager
from .database_tools import create_database_tools, get_database_tool_descriptions

__all__ = [
    'DatabaseManager',
    'create_database_tools',
    'get_database_tool_descriptions',
]