#!/bin/bash

# Snowflake MCP Inspector Development Script
# This script starts the MCP server and launches the inspector for testing

echo "🔍 Starting Snowflake MCP Server with Inspector"
echo ""

# Set script directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Check if .env file exists
if [ ! -f .env ]; then
    echo "❌ No .env file found"
    echo "💡 Please create a .env file with your Snowflake credentials:"
    echo "   SNOWFLAKE_ACCOUNT=your-account"
    echo "   SNOWFLAKE_USER=your-username"
    echo "   SNOWFLAKE_PASSWORD=your-password  # For OAuth/password auth"
    exit 1
fi

# Load environment variables
echo "📋 Loading environment variables from .env"
set -a
source .env
set +a

# Check required Snowflake environment variables
required_vars=("SNOWFLAKE_ACCOUNT")
missing_vars=()

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        missing_vars+=($var)
    fi
done

if [ ${#missing_vars[@]} -gt 0 ]; then
    echo "❌ Missing required environment variables: ${missing_vars[*]}"
    echo "💡 Please set these in your .env file"
    exit 1
fi

# Check for Node.js and npm
if ! command -v npm &> /dev/null; then
    echo "❌ npm is required for MCP Inspector"
    echo "💡 Please install Node.js from https://nodejs.org/"
    exit 1
fi

# Check if uvx is available
if ! command -v uvx &> /dev/null; then
    echo "📦 Installing uv tools..."
    pip install uv
fi

# Ensure all dependencies are installed, including snowflake-core
echo "📦 Syncing dependencies (including snowflake-core for database tools)..."
uv sync --all-extras --dev
if [ $? -ne 0 ]; then
    echo "⚠️  Failed to sync dependencies. Trying direct install of snowflake-core..."
    uv pip install snowflake-core
fi

# Install the local package in editable mode to use our changes
echo "📦 Installing local package in editable mode..."
uv pip install -e .

echo "❄️  Snowflake account: $SNOWFLAKE_ACCOUNT"
echo ""

# Build the MCP server command with STDIO transport
# Now using the locally installed package (installed with -e above)
MCP_CMD="uv run mcp-server-snowflake --transport stdio"

# Add optional parameters if they exist
if [ -n "$SNOWFLAKE_USER" ]; then
    MCP_CMD="$MCP_CMD --user $SNOWFLAKE_USER"
fi

if [ -n "$SNOWFLAKE_ROLE" ]; then
    MCP_CMD="$MCP_CMD --role $SNOWFLAKE_ROLE"
fi

if [ -n "$SNOWFLAKE_DATABASE" ]; then
    MCP_CMD="$MCP_CMD --database $SNOWFLAKE_DATABASE"
fi

if [ -n "$SNOWFLAKE_SCHEMA" ]; then
    MCP_CMD="$MCP_CMD --schema $SNOWFLAKE_SCHEMA"
fi

if [ -n "$SNOWFLAKE_WAREHOUSE" ]; then
    MCP_CMD="$MCP_CMD --warehouse $SNOWFLAKE_WAREHOUSE"
fi

# Use the specific config file at the root of the project
CONFIG_FILE="$SCRIPT_DIR/tools_config.yaml"
if [ -f "$CONFIG_FILE" ]; then
    echo "📄 Using service config: $CONFIG_FILE"
    MCP_CMD="$MCP_CMD --service-config-file $CONFIG_FILE"
else
    # Fallback to other locations if the main one doesn't exist
    if [ -f "services/tools_config.yaml" ]; then
        echo "📄 Using service config: services/tools_config.yaml"
        MCP_CMD="$MCP_CMD --service-config-file services/tools_config.yaml"
    elif [ -f "$SCRIPT_DIR/services/tools_config.yaml" ]; then
        echo "📄 Using service config: $SCRIPT_DIR/services/tools_config.yaml"
        MCP_CMD="$MCP_CMD --service-config-file $SCRIPT_DIR/services/tools_config.yaml"
    else
        echo "⚠️  No service config file found. Server may not start properly."
        echo "💡 Looking for: $CONFIG_FILE"
    fi
fi

echo "🚀 Starting MCP Inspector..."
echo "💡 This will open a web interface to test your Snowflake MCP server"
echo ""
echo "📌 Available tools:"
echo "   • Cortex Search (if configured)"
echo "   • Cortex Analyst (if configured)"
echo "   • create_database (when integrated)"
echo "   • list_databases (when integrated)"
echo "   • drop_database (when integrated)"
echo ""
echo "🔧 Running command:"
echo "   npx @modelcontextprotocol/inspector $MCP_CMD"
echo ""
echo "⏳ Starting inspector (this may take a moment if packages need to be installed)..."
echo ""

# Run the MCP Inspector with our server command
npx @modelcontextprotocol/inspector $MCP_CMD

echo ""
echo "✅ Inspector session ended"