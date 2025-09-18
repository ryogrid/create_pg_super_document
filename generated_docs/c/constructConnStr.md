# constructConnStr

## Location
src/bin/pg_dump/pg_dumpall.c: 1946 - 1976

## Overview
Constructs a connection string from given keyword/value pairs, specifically designed to pass connection options to pg_dump subprocess while excluding sensitive or variable parameters.

## Definition
```c
static char *constructConnStr(const char **keywords, const char **values)
```

## Detailed Description
This function builds a PostgreSQL connection string in key='value' format from arrays of connection parameter keywords and their corresponding values. It is specifically used by pg_dumpall to create connection strings that can be safely passed to pg_dump subprocesses. The function implements security and functionality considerations by explicitly excluding certain parameters:

- **dbname**: Excluded because it varies for each pg_dump invocation
- **password**: Excluded for security reasons (passwords should not be passed on command line)  
- **fallback_application_name**: Excluded to let pg_dump set its own application name

The function creates a PQExpBuffer, iterates through the keyword/value pairs, and constructs a properly formatted connection string with appropriate spacing and value escaping.

## Parameters / Member Variables
- `keywords`: Array of connection parameter keywords (null-terminated)
- `values`: Array of corresponding parameter values (null-terminated)

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - appendPQExpBufferChar
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendConnStrVal](../a/appendConnStrVal.md)
  - [pg_strdup](../p/pg_strdup.md)
  - destroyPQExpBuffer
- Called from (representative examples):
  - [connectDatabase](connectDatabase.md)

## Notes and Other Information
- This is a static function within pg_dumpall.c, indicating it's only used internally within that module
- The function ensures proper escaping of connection string values through appendConnStrVal
- Memory management is handled properly with PQExpBuffer creation and destruction
- The exclusion of sensitive parameters makes this suitable for command-line argument passing
- Part of the pg_dumpall utility's database connection management subsystem