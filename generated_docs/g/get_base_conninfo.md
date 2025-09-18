# get_base_conninfo

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 271 - 314

## Overview
Parses and validates a PostgreSQL connection string, returning a base connection string with the database name removed, optionally extracting the database name separately.

## Definition


## Detailed Description
This function processes a PostgreSQL connection string to create a "base" connection string that can be reused for connecting to multiple databases. It parses the input connection string using libpq's PQconninfoParse(), extracts all connection parameters except the database name, and reconstructs them into a new connection string.

The function serves a key role in pg_createsubscriber where the same connection parameters (host, port, user, etc.) need to be used to connect to different databases. By separating the database name from other connection parameters, it enables efficient connection string reuse.

Key operations:
1. Parses the input connection string and validates its syntax
2. Iterates through all connection parameters
3. Extracts the database name separately if requested
4. Rebuilds all other parameters into a new connection string
5. Returns the base connection string and optionally the database name

## Parameters / Member Variables
- : Input PostgreSQL connection string to be parsed and processed
- : Optional output parameter - if non-NULL, receives a copy of the database name extracted from the connection string (caller must free)

## Dependencies
- Functions called/Symbols referenced:
  - PQconninfoOption (libpq structure for connection options)
  - PQconninfoParse (parses connection string into options array)
  - PQfreemem (frees libpq-allocated memory)
  - appendConnStrItem (helper to build connection string items)
  - PQconninfoFree (frees connection options array)
  - pg_log_error, pg_strdup, createPQExpBuffer, destroyPQExpBuffer
- Called from (representative examples):
  - main (in pg_createsubscriber main function)

## Notes and Other Information
- This is a static function specific to pg_createsubscriber utility
- Returns NULL on parsing errors and logs appropriate error messages
- Caller is responsible for freeing both the returned connection string and the dbname if provided
- Critical for the multi-database operation mode of pg_createsubscriber
- Handles empty or NULL connection parameter values appropriately
- Uses libpq's standard connection string parsing to ensure compatibility
- The returned base connection string can be combined with different database names using other helper functions