# describeAccessMethods

## Location
src/bin/psql/describe.c: 141 - 214

## Overview
Implements the \dA psql command to display a list of access methods in the database, supporting both index and table access methods with optional verbose output.

## Definition
```c
bool describeAccessMethods(const char *pattern, bool verbose)
```

## Detailed Description
This function generates and executes a SQL query to list access methods from the pg_am system catalog. It handles PostgreSQL version compatibility by checking for minimum version 9.6 (when access methods were introduced). The function constructs a query that shows access method names and types (Index or Table), with optional verbose information including handler functions and descriptions. It supports pattern-based filtering and provides internationalized output with proper column translation.

## Parameters / Member Variables
- `pattern`: Optional regular expression pattern to filter access methods by name
- `verbose`: Boolean flag to include additional columns (handler function and description)

## Dependencies
- Functions called/Symbols referenced:
  - formatPGVersionNumber
  - initPQExpBuffer
  - printfPQExpBuffer
  - appendPQExpBuffer
  - appendPQExpBufferStr
  - validateSQLNamePattern
  - termPQExpBuffer
  - PSQLexec
  - printQuery
  - PQclear
  - lengthof
- Called from (representative examples):
  - exec_command_d (in command.c:813)

## Notes and Other Information
- Part of psql's describe functionality (\dA command)
- Requires PostgreSQL version 9.6 or later (access methods were introduced in this version)
- Returns early with an error message for unsupported server versions
- Uses static column translation array for proper internationalization
- Distinguishes between index ('i') and table ('t') access method types
- In verbose mode, shows handler function and description from pg_am catalog