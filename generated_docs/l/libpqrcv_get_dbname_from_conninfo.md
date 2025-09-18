# libpqrcv_get_dbname_from_conninfo

## Location
src/backend/replication/libpqwalreceiver/libpqwalreceiver.c: 502 - 550

## Overview
Extracts the database name from a PostgreSQL connection string used for WAL receiver operations, returning NULL if no database name is specified.

## Definition


## Detailed Description
This function parses a PostgreSQL connection string to extract the database name parameter. It utilizes libpq's connection info parsing functionality to break down the connection string into its component parameters and searches specifically for the "dbname" parameter. The function handles multiple dbname specifications by returning the last one found, and properly manages memory allocation and error handling during the parsing process.

The function is particularly useful in replication scenarios where the WAL receiver needs to determine which database it should connect to based on the connection information provided. It performs robust error handling for invalid connection strings and ensures proper memory management throughout the parsing operation.

## Parameters / Member Variables
- : Null-terminated string containing the PostgreSQL connection information to parse

## Dependencies
- Functions called/Symbols referenced:
  - PQconninfoParse
  - PQconninfoFree
  - PQfreemem
  - pstrdup
  - pfree
  - strcmp
- Called from (representative examples):
  - WalReceiverConn (referenced in connection management routines)

## Notes and Other Information
- This is a static function internal to the libpqwalreceiver module
- Returns a dynamically allocated string that must be freed by the caller, or NULL if no dbname is found
- If multiple dbname parameters are present in the connection string, the last one takes precedence
- The function will raise an ERROR if the connection string has invalid syntax
- Properly handles memory management for both successful parsing and error conditions
- Located at src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:502-550