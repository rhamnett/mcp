# Snowflake Cortex AI Model Context Protocol (MCP) Server

<a href="https://emerging-solutions-toolbox.streamlit.app/">
    <img src="https://github.com/user-attachments/assets/aa206d11-1d86-4f32-8a6d-49fe9715b098" alt="image" width="150" align="right";">
</a>

This Snowflake MCP server provides tooling for Snowflake Cortex AI, object management, and SQL orchestration, bringing these capabilities to the MCP ecosystem. When connected to an MCP Client (e.g. [Claude for Desktop](https://claude.ai/download), [fast-agent](https://fast-agent.ai/), [Agentic Orchestration Framework](https://github.com/Snowflake-Labs/orchestration-framework/blob/main/README.md)), users can leverage these features.

The MCP server currently supports the below capabilities:
- **[Cortex Search](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)**: Query unstructured data in Snowflake as commonly used in Retrieval Augmented Generation (RAG) applications.
- **[Cortex Analyst](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst)**: Query structured data in Snowflake via rich semantic modeling.
- **[Cortex Agent](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents)**: Agentic orchestrator across structured and unstructured data retrieval
- **Object Management**: Perform basic operations against Snowflake's most common objects such as creation, dropping, updating, and more.
- **SQL Execution**: Run LLM-generated SQL managed by user-configured permissions.
- **[Semantic View Querying](https://docs.snowflake.com/en/user-guide/views-semantic/overview)**: Discover and query Snowflake Semantic Views

# Getting Started

## Service Configuration

A simple configuration file is used to drive all tooling. An example can be seen at [services/configuration.yaml](services/configuration.yaml) and a template is below. The path to this configuration file will be passed to the server and the contents used to create MCP server tools at startup.

**Cortex Services**

Many Cortex Agent, Search, and Analyst services can be added. Ideal descriptions are both highly descriptive and mutually exclusive.
Only the explicitly listed Cortex services will be available as tools in the MCP client.

**Other Services**

Other services include tooling for [object management](object-management), [query execution](sql-execution), and [semantic view usage](semantic-view-querying).
These groups of tools can be enabled by setting them to True in the `other_services` section of the configuration file.

**SQL Statement Permissions**

The `sql_statement_permissions` section ensures that only approved statements are executed across any tools with access to change Snowflake objects.
The list contains SQL expression types. Those marked with True are permitted while those marked with False are not permitted. Please see [SQL Execution](#sql-execution) for examples of each expression type.

```
agent_services: # List all Cortex Agent services
  - service_name: "<service_name>"
    description: > # Describe contents of the agent service"
      "<Agent service that ...>"
    database_name: "<database_name>"
    schema_name: "<schema_name>"
  - service_name: "<service_name>"
    description: > # Describe contents of the agent service"
      "<Agent service that ...>"
    database_name: "<database_name>"
    schema_name: "<schema_name>"
search_services: # List all Cortex Search services
  - service_name: "<service_name>"
    description: > # Describe contents of the search service"
      "<Search services that ...>"
    database_name: "<database_name>"
    schema_name: "<schema_name>"
  - service_name: "<service_name>"
    description: > # Describe contents of the search service"
      "<Search services that ...>"
    database_name: "<database_name>"
    schema_name: "<schema_name>"
analyst_services: # List all Cortex Analyst semantic models/views
  - service_name: "<service_name>" # Create descriptive name for the service
    semantic_model: "<semantic_yaml_or_view>" # Fully-qualify semantic YAML model or Semantic View
    description: > # Describe contents of the analyst service"
      "<Analyst service that ...>"
  - service_name: "<service_name>" # Create descriptive name for the service
    semantic_model: "<semantic_yaml_or_view>" # Fully-qualify semantic YAML model or Semantic View
    description: > # Describe contents of the analyst service"
      "<Analyst service that ...>"
other_services: # Set desired tool groups to True to enable tools for that group
  object_manager: True # Perform basic operations against Snowflake's most common objects such as creation, dropping, updating, and more.
  query_manager: True # Run LLM-generated SQL managed by user-configured permissions.
  semantic_manager: True # Discover and query Snowflake Semantic Views and their components.
sql_statement_permissions: # List SQL statements to explicitly allow (True) or disallow (False).
  # - All: True # To allow everything, uncomment and set All: True.
  - Alter: True
  - Command: True
  - Comment: True
  - Commit: True
  - Create: True
  - Delete: True
  - Describe: True
  - Drop: True
  - Insert: True
  - Merge: True
  - Rollback: True
  - Select: True
  - Transaction: True
  - TruncateTable: True
  - Unknown: False # To allow unknown or unmapped statement types, set Unknown: True.
  - Update: True
  - Use: True
```

> [!NOTE]
> Previous versions of the configuration file supported specifying explicit values for columns and limit for each Cortex Search service. Instead, these are now exclusively dynamic based on user prompt. If not specified, a search service's default search_columns will be returned with a limit of 10.

## Connecting to Snowflake

The MCP server uses the [Snowflake Python Connector](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect) for all authentication and connection methods. **Please refer to the official Snowflake documentation for comprehensive authentication options and best practices.**

**The MCP server honors the RBAC permissions assigned to the specified role (as passed in the connection parameters) or default role of the user (if no role is passed to connect).**

Connection parameters can be passed as CLI arguments and/or environment variables. The server supports all authentication methods available in the Snowflake Python Connector, including:

- Username/password authentication
- Key pair authentication
- OAuth authentication
- Single Sign-On (SSO)
- Multi-factor authentication (MFA)

### Connection Parameters

Connection parameters can be passed as CLI arguments and/or environment variables:

| Parameter | CLI Arguments | Environment Variable | Description |
|-----------|--------------|---------------------|-------------|
| Account | --account | SNOWFLAKE_ACCOUNT | Account identifier (e.g. xy12345.us-east-1) |
| Host | --host | SNOWFLAKE_HOST | Snowflake host URL |
| User | --user, --username | SNOWFLAKE_USER | Username for authentication |
| Password | --password | SNOWFLAKE_PASSWORD | Password or programmatic access token |
| Role | --role | SNOWFLAKE_ROLE | Role to use for connection |
| Warehouse | --warehouse | SNOWFLAKE_WAREHOUSE | Warehouse to use for queries |
| Passcode in Password | --passcode-in-password | - | Whether passcode is embedded in password |
| Passcode | --passcode | SNOWFLAKE_PASSCODE | MFA passcode for authentication |
| Private Key | --private-key | SNOWFLAKE_PRIVATE_KEY | Private key for key pair authentication |
| Private Key File | --private-key-file | SNOWFLAKE_PRIVATE_KEY_FILE | Path to private key file |
| Private Key Password | --private-key-file-pwd | SNOWFLAKE_PRIVATE_KEY_FILE_PWD | Password for encrypted private key |
| Authenticator | --authenticator | - | Authentication type (default: snowflake) |
| Connection Name | --connection-name | - | Name of connection from connections.toml (or config.toml) file |

> [!WARNING]
> **Deprecation Notice**: The CLI arguments `--account-identifier` and `--pat`, as well as the environment variable `SNOWFLAKE_PAT`, are deprecated and will be removed in a future release. Please use `--account` and `--password` (or `SNOWFLAKE_ACCOUNT` and `SNOWFLAKE_PASSWORD`) instead.

# Using with MCP Clients

The MCP server is client-agnostic and will work with most MCP Clients that support basic functionality for MCP tools and (optionally) resources. Below are some examples.

## [Claude Desktop](https://support.anthropic.com/en/articles/10065433-installing-claude-for-desktop)
To integrate this server with Claude Desktop as the MCP Client, add the following to your app's server configuration. By default, this is located at
- macOS: ~/Library/Application Support/Claude/claude_desktop_config.json
- Windows: %APPDATA%\Claude\claude_desktop_config.json

Set the path to the service configuration file and configure your connection method.

```
{
  "mcpServers": {
    "mcp-server-snowflake": {
      "command": "uvx",
      "args": [
        "snowflake-labs-mcp",
        "--service-config-file",
        "<path to file>/tools_config.yaml",
        "--connection-name",
        "default"
      ]
    }
  }
}
```
## [Cursor](https://www.cursor.com/)
Register the MCP server in cursor by opening Cursor and navigating to Settings -> Cursor Settings ->  MCP. Add the below.
```
{
  "mcpServers": {
    "mcp-server-snowflake": {
      "command": "uvx",
      "args": [
        "snowflake-labs-mcp",
        "--service-config-file",
        "<path to file>/tools_config.yaml",
        "--connection-name",
        "default"
      ]
    }
  }
}
```

Add the MCP server as context in the chat.

<img src="https://sfquickstarts.s3.us-west-1.amazonaws.com/misc/mcp/Cursor.gif" width="800"/>

For troubleshooting Cursor server issues, view the logs by opening the Output panel and selecting Cursor MCP from the dropdown menu.

## [fast-agent](https://fast-agent.ai/)

Update the `fastagent.config.yaml` mcp server section with the configuration file path and connection name.
```
# MCP Servers
mcp:
    servers:
        mcp-server-snowflake:
            command: "uvx"
            args: ["snowflake-labs-mcp", "--service-config-file", "<path to file>/tools_config.yaml", "--connection-name", "default"]
```

<img src="https://sfquickstarts.s3.us-west-1.amazonaws.com/misc/mcp/fast-agent.gif" width="800"/>

## Microsoft Visual Studio Code + GitHub Copilot

For prerequisites, environment setup, step-by-step guide and instructions, please refer to this [blog](https://medium.com/snowflake/build-a-natural-language-data-assistant-in-vs-code-with-copilot-mcp-and-snowflake-cortex-ai-04a22a3b0f17).

<img src="https://sfquickstarts.s3.us-west-1.amazonaws.com/misc/mcp/dash-dark-mcp-copilot.gif"/>

# Cortex Services

Instances of Cortex Agent (in `agent_services` section), Cortex Search (in `search_services` section), and Cortex Analyst (in `analyst_services` section) of the configuration file will be served as tools. Leave these sections blank to omit such tools.

Only Cortex Agent objects are supported in the MCP server. That is, only Cortex Agent objects pre-configured in Snowflake can be leveraged as tools. See [Cortex Agent Run API](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-run#streaming-responses) for more details.

Ensure all services have accurate context names for service name, database, schema, etc. Ideal descriptions are both highly descriptive and mutually exclusive.

The `semantic_model` value in analyst services should be a fully-qualified semantic view OR semantic YAML file in a Snowflake stage:
- For a semantic view: `MY_DATABASE.MY_SCHEMA.MY_SEMANTIC_VIEW`
- For a semantic YAML file: `@MY_DATABASE.MY_SCHEMA.MY_STAGE/my_semantic_file.yaml` (**Note the `@`.**)

# Object Management

The MCP server includes dozens of tools narrowly scoped to fulfill basic operation management. It is recommended to use Snowsight directly for advanced object management.

The MCP server currently supports **creating**, **dropping**, **creating or altering**, **describing**, and **listing** the below object types.
**To enable these tools, set `object_manager` to True in the configuration file under `other_services`.**

```
- Database
- Schema
- Table
- View
- Warehouse
- Compute Pool
- Role
- Stage
- User
- Image Repository
```

Please note that these tools are also governed by permissions captured in the configuration file under `sql_statement_permissions`.
Object management tools to create and create or alter objects are governed by the `Create` permission. Object dropping is governed by the `Drop` permission.

It is likely that more actions and objects will be included in future releases.

# SQL Execution

The general SQL tool will provide a way to execute generic SQL statements generated by the MCP client. Users have full control over the types of SQL statement that are approved in the configuration file.

Listed in the configuration file under `sql_statement_permissions` are [sqlglot expression types](https://sqlglot.com/sqlglot/expressions.html). Those marked as False will be stopped before execution. Those marked with True will be executed (or prompt the user for execution based on the MCP client settings).

**To enable the SQL execution tool, set `query_manager` to True in the configuration file under `other_services`.**
**To allow all SQL expressions to pass the additional validation, set `All` to True.**

Not all Snowflake SQL commands are mapped in sqlglot and you may find some obscure commands have yet to be captured in the configuration file.
**Setting `Unknown` to True will allow these uncaptured commands to pass the additional validation.** You may also add new expression types directly to honor specific ones.

Below are some examples of sqlglot expression types with accompanying Snowflake SQL command examples:

| SQLGlot Expression Type | SQL Command |
|------------------------|-------------|
| Alter | `ALTER TABLE my_table ADD COLUMN new_column VARCHAR(50);` |
| Command | `CALL my_procedure('param1_value', 123);`<br/>`GRANT ROLE analyst TO USER user1;`<br/>`SHOW TABLES IN SCHEMA my_database.my_schema;` |
| Comment | `COMMENT ON TABLE my_table IS 'This table stores customer data.';` |
| Commit | `COMMIT;` |
| Create | `CREATE TABLE my_table ( id INT, name VARCHAR(255), email VARCHAR(255) );`<br/>`CREATE OR ALTER VIEW my_schema.my_new_view AS SELECT id, name, created_at FROM my_schema.my_table WHERE created_at >= '2023-01-01';` |
| Delete | `DELETE FROM my_table WHERE id = 101;` |
| Describe | `DESCRIBE TABLE my_table;` |
| Drop | `DROP TABLE my_table;` |
| Error | `COPY INTO my_table FROM @my_stage/data/customers.csv FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_DELIMITER = ',');`<br/>`REVOKE ROLE analyst FROM USER user1;`<br/>`UNDROP TABLE my_table;` |
| Insert | `INSERT INTO my_table (id, name, email) VALUES (102, 'Jane Doe', 'jane.doe@example.com');` |
| Merge | `MERGE INTO my_table AS target USING (SELECT 103 AS id, 'John Smith' AS name, 'john.smith@example.com' AS email) AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET target.name = source.name, target.email = source.email WHEN NOT MATCHED THEN INSERT (id, name, email) VALUES (source.id, source.name, source.email);` |
| Rollback | `ROLLBACK;` |
| Select | `SELECT id, name FROM my_table WHERE id < 200 ORDER BY name;` |
| Transaction | `BEGIN;` |
| TruncateTable | `TRUNCATE TABLE my_table;` |
| Update | `UPDATE my_table SET email = 'new.email@example.com' WHERE name = 'Jane Doe';` |
| Use | `USE DATABASE my_database;` |

# Semantic View Querying

Several tools support the discovery and querying of [Snowflake Semantic Views](https://docs.snowflake.com/en/user-guide/views-semantic/overview) and their components.
Semantic Views can be **listed** and **described**. In addition, you can **list their metrics and dimensions**.
Lastly, you can **[query Semantic Views](https://docs.snowflake.com/en/user-guide/views-semantic/querying)** directly.

**To enable these tools, set `semantic_manager` to True in the configuration file under `other_services`.**

# Troubleshooting

## Running MCP Inspector

The [MCP Inspector](https://modelcontextprotocol.io/docs/tools/inspector) is suggested for troubleshooting the MCP server. Run the below to launch the inspector.

`npx @modelcontextprotocol/inspector uvx snowflake-labs-mcp --service-config-file "<path_to_file>/tools_config.yaml" --connection-name "default"`

# FAQs

#### How do I connect to Snowflake?

- The MCP server supports all connection methods supported by the Snowflake Python Connector.
See [Connecting to Snowflake with the Python Connector](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect) for more information.

#### I'm receiving a tool limit error/warning.

- While LLMs' support for more tools will likely grow, you can hide tool groups by setting them to False in the configuration file.
Only listed Cortex services will be made into tools as well.

#### Can I use a Programmatic Access Token (PAT) instead of a password?

- Yes. Pass it to the CLI flag --password or set as environment variable SNOWFLAKE_PASSWORD.

#### How do I try this?

- The MCP server is intended to be used as one part of the MCP ecosystem. Think of it as a collection of tools. You'll need an MCP Client to act as an orchestrator. See the [MCP Introduction](https://modelcontextprotocol.io/introduction) for more information.

#### Where is this deployed? Is this in Snowpark Container Services?

- All tools in this MCP server are managed services, accessible via REST API. No separate remote service deployment is necessary. Instead, the current version of the server is intended to be started by the MCP client, such as Claude Desktop, Cursor, fast-agent, etc. By configuring these MCP client with the server, the application will spin up the server service for you. Future versions of the MCP server may be deployed as a remote service in the future.

#### I'm receiving permission errors from my tool calls.

- If using a Programmatic Access Tokens, note that they do not evaluate secondary roles. When creating them, please select a single role that has access to all services and their underlying objects OR select any role. A new PAT will need to be created to alter this property.

#### How many Cortex Search or Cortex Analysts can I add?

- You may add multiple instances of both services. The MCP Client will determine the appropriate one(s) to use based on the user's prompt.

#### Help! I'm getting an SSLError?

- If your account name contains underscores, try using the dashed version of the URL.
  - Account identifier with underscores: `acme-marketing_test_account`
  - Account identifier with dashes: `acme-marketing-test-account`

# Bug Reports, Feedback, or Other Questions

Please add issues to the GitHub repository.

<!-- mcp-name: io.github.Snowflake-Labs/mcp -->
