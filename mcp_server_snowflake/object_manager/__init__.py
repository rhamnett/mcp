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

This module provides unified object management capabilities using the 
CoreObjectManager pattern and registry-based configuration for all
Snowflake objects.
"""

from .managers.core import CoreObjectManager
from .registry import CORE_REGISTRY, get_object_config
from .dynamic import create_dynamic_tools
from .tools import create_object, list_objects, drop_object

# Keep legacy imports for backward compatibility during migration
try:
    from .database_manager import DatabaseManager
    from .database_tools import create_database_tools, get_database_tool_descriptions
    _legacy_available = True
except ImportError:
    _legacy_available = False

__all__ = [
    'CoreObjectManager',
    'create_dynamic_tools',
    'create_object',
    'list_objects',
    'drop_object',
    'CORE_REGISTRY',
    'get_object_config',
]

# Add legacy exports if available
if _legacy_available:
    __all__.extend([
        'DatabaseManager',
        'create_database_tools',
        'get_database_tool_descriptions',
    ])