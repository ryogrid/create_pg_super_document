# PQconndefaults

## Location
src/interfaces/libpq/fe-connect.c: 1881 - 1918

## Overview
Constructs a default connection options array that identifies all available connection options and shows default values from environment variables and system settings.

## Definition


## Detailed Description
This function creates and returns a dynamically allocated array of PQconninfoOption structures that contains all possible PostgreSQL connection parameters with their current default values. The defaults are determined from environment variables, system configuration, and built-in defaults. This function is useful for applications that need to discover all available connection options and their current default values before establishing a connection.

The function performs the following operations:
- Initializes an error buffer for internal operations
- Calls conninfo_init() to create the basic connection options structure
- Calls conninfo_add_defaults() to populate default values from environment and system settings
- Handles memory allocation failures gracefully
- Returns a dynamically allocated array that must be freed with PQconninfoFree()

The returned array is terminated by an entry with a NULL keyword field.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
  - PQExpBufferDataBroken
  - conninfo_init
  - conninfo_add_defaults
  - PQconninfoFree
  - termPQExpBuffer
- Called from (representative examples):
  - GetDbnameFromConnectionOptions (pg_basebackup)
  - check_pghost_envvar (pg_upgrade)
  - do_connect (psql)
  - main (libpq_uri_regress test)

## Notes and Other Information
- Returns NULL on error (typically out of memory)
- The returned array is dynamically allocated and must be freed with PQconninfoFree()
- Prior to PostgreSQL 7.0, this function returned a static array, which was not thread-safe
- Applications using this function should always call PQconninfoFree() to avoid memory leaks
- The function doesn't report specific errors but handles them internally
- Each option in the returned array contains keyword, environment variable name, compiled default, current value, label, and display information