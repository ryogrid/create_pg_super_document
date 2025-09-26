# ECPGprepared_statement

## Location
src/interfaces/ecpg/ecpglib/prepare.c: 368 - 378

## Overview
Retrieves a previously prepared SQL statement by name from the statement cache for a given database connection.

## Definition
```c
char *ECPGprepared_statement(const char *connection_name, const char *name, int lineno)
```

## Detailed Description
This function serves as a public API wrapper for retrieving prepared SQL statements in ECPG (Embedded C for PostgreSQL). It takes a connection name and statement name, resolves the connection, and delegates to the internal `ecpg_prepared` function to retrieve the actual prepared statement text. The function includes a line number parameter for API compatibility, though this parameter is not actively used in the current implementation.

## Parameters / Member Variables
- `connection_name`: Name of the database connection to search for the prepared statement
- `name`: Name identifier of the prepared statement to retrieve
- `lineno`: Line number parameter maintained for API compatibility (currently unused)

## Dependencies
- Functions called/Symbols referenced:
  - ecpg_get_connection
  - ecpg_prepared
- Called from (representative examples):
  - Various ECPG test programs and applications
  - Generated ECPG code from precompiled embedded SQL

## Notes and Other Information
- The lineno parameter is explicitly marked as unused to suppress compiler warnings
- This function is part of the ECPG library public API for prepared statement management
- Returns the prepared statement text as a string, or NULL if not found
- Widely used in ECPG test suite and generated embedded SQL code