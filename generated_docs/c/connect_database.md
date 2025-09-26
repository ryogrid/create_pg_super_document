# connect_database

## Location
[src/bin/pg_basebackup/pg_createsubscriber.c:505-544](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/pg_createsubscriber.c#L505-L544)

## Overview
Establishes a new PostgreSQL database connection with secure configuration and provides flexible error handling based on the caller's requirements.

## Definition

```c
static PGconn *
connect_database(const char *conninfo, bool exit_on_error)
```
## Detailed Description
The  function is a centralized database connection utility used throughout pg_createsubscriber. It establishes a connection using the provided connection string and performs essential security configuration by clearing the search_path to prevent potential security issues.

The function provides flexible error handling through the  parameter. When set to true, any connection failure or security configuration failure will cause the entire program to exit immediately. When set to false, the function returns NULL on failure, allowing the caller to handle the error gracefully or attempt alternative connections.

After establishing the connection, the function automatically executes  to secure the search_path, which is a standard security practice to prevent unauthorized code execution through search_path manipulation.

## Parameters
- : The PostgreSQL connection string specifying connection parameters (host, port, database, credentials, etc.)
- : Boolean flag controlling error handling behavior - if true, exits the program on failure; if false, returns NULL on failure

## Dependencies
- Functions called/Symbols referenced:
  -  - Establishes the database connection using libpq
  -  - Checks the connection status
  -  - Closes the database connection on failure
  -  - Executes the search_path security command
  -  - Checks the result status of the security command
  -  - Retrieves error messages on failure
  -  - Frees the result object
  -  - Logs connection and security configuration errors
- Constants referenced:
  -  - Expected status for successful connections
  -  - Expected result status for the search_path command
  -  - SQL command to secure the search_path
- Called from (major functions):
  - , , , , , , , , , 

## Notes and Other Information
- The function is marked as , indicating it's only used within the pg_createsubscriber.c file
- Always secures the search_path immediately after connection to prevent security vulnerabilities
- Returns a valid PGconn pointer on success, or NULL on failure (when exit_on_error is false)
- Proper cleanup is performed on all error paths to avoid resource leaks
- The dual error handling mode makes it suitable for both critical connections (where failure should abort) and optional connections (where failure can be handled)
- Used extensively throughout the pg_createsubscriber workflow for various database operations
- Connection cleanup on error paths ensures no resource leaks occur