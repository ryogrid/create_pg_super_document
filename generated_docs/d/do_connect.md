# do_connect

## Location
[src/bin/psql/command.c:3386-3849](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L3386-L3849)

## Overview
Handles the \connect command in psql, establishing a new database connection with given parameters while optionally reusing parameters from the previous connection.

## Definition


## Detailed Description
The  function is the core handler for psql's \connect command, responsible for establishing database connections with specified parameters. It supports both traditional parameter-based connections and connection string formats. The function intelligently manages parameter reuse from previous connections and handles password authentication, including prompting for passwords when needed.

Key behaviors include:
- **Parameter reuse logic**: When reusing previous connection parameters, it extracts them from the current or dead connection and selectively replaces them with new values
- **Connection string support**: Parses PostgreSQL connection strings and URIs, validating that additional parameters aren't provided when using connection strings
- **Password management**: Implements sophisticated password reuse logic based on whether connection-critical parameters (user, host, port) have changed
- **Interactive vs non-interactive modes**: In interactive mode, failed connections preserve the previous connection; in scripting mode, failed connections close all connections
- **Client encoding handling**: Automatically sets client_encoding to "auto" for terminal connections without PGCLIENTENCODING

## Parameters / Member Variables
- : Controls whether to reuse parameters from previous connection (TRI_YES, TRI_NO, or TRI_DEFAULT)
- : Target database name or connection string/URI
- : Username for authentication (can be NULL to reuse previous or use defaults)
- : Database server hostname (can be NULL to reuse previous or use defaults) 
- : Database server port (can be NULL to reuse previous or use defaults)

## Dependencies
- Functions called/Symbols referenced:
  - : Identifies if dbname is a connection string
  - /: Retrieves connection parameters
  - : Parses connection strings
  - : Interactive password prompting
  - : Initiates database connection
  - : Waits for connection completion
  - : Displays connection warnings
  - : Synchronizes psql variables with new connection
- Called from (representative examples):
  - : Main \connect command handler

## Notes and Other Information
- The function implements a retry loop for password authentication, allowing users to correct failed password attempts
- Connection parameter precedence follows: command parameters > connection string values > previous connection values > libpq defaults
- Memory management is carefully handled with proper cleanup of libpq structures and locally allocated data
- The function preserves behavioral consistency between interactive and scripting modes while providing appropriate user feedback