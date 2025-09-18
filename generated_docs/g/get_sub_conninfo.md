# get_sub_conninfo

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 315 - 340

## Overview
Builds a connection string for connecting to the subscriber (target) PostgreSQL server during pg_createsubscriber operations with restricted parameters suitable for local connections.

## Definition


## Detailed Description
This function constructs a PostgreSQL connection string specifically for connecting to the subscriber database server. Unlike general-purpose connection strings, it only includes a limited set of parameters that are appropriate for local connections to a server that is being set up with restricted access during the subscription creation process.

The function builds a connection string with:
1. Port number from the subscriber options
2. Host/socket directory (Unix domain socket on non-Windows platforms)
3. Username (if specified in options)
4. Application name set to the program name for identification

This specialized connection string is designed for the specific use case of pg_createsubscriber where the tool needs to connect to a locally managed PostgreSQL instance that may have restricted connection settings.

## Parameters / Member Variables
- : Pointer to CreateSubscriberOptions structure containing configuration for the subscriber setup, including:
  - : Port number for the subscriber server
  - : Directory for Unix domain sockets (non-Windows only)
  - : Optional username for subscriber connections

## Dependencies
- Functions called/Symbols referenced:
  - [CreateSubscriberOptions](../C/CreateSubscriberOptions.md) (structure type for subscriber configuration)
  - [appendConnStrItem](../a/appendConnStrItem.md) (helper function to build connection string items)
  - createPQExpBuffer, destroyPQExpBuffer (buffer management)
  - [pg_strdup](../p/pg_strdup.md) (string duplication)
  - progname (global variable with program name)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_createsubscriber main function)

## Notes and Other Information
- This is a static function specific to pg_createsubscriber utility
- Uses conditional compilation (#if !defined(WIN32)) to handle platform differences for Unix domain sockets
- Returns a newly allocated string that the caller must free
- Intentionally limited parameter set compared to full PostgreSQL connection strings
- Sets fallback_application_name to help identify connections in server logs
- Designed for local connections to subscriber servers with restricted access during setup
- The returned connection string is used for administrative operations on the target/subscriber database