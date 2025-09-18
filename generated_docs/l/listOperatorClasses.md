# listOperatorClasses

## Location
src/bin/psql/describe.c: 6677 - 6777

## Overview
The  function implements the  psql command to display a formatted list of operator classes, with optional filtering by index access method and input data type.

## Definition


## Detailed Description
This function constructs and executes an SQL query to retrieve operator class information from PostgreSQL system catalogs. It displays operator classes with their associated access methods, input types, storage types (when different from input type), names, and default status. In verbose mode, it additionally shows the operator family and owner information. The function supports pattern matching for filtering results by access method name and type name, using PostgreSQL's standard pattern matching syntax.

The query joins multiple system catalogs (, , , , and optionally ) to gather comprehensive information about operator classes. Results are sorted by access method, input type, and operator class name for consistent presentation.

## Parameters / Member Variables
- : Optional regex pattern to filter results by index access method name (e.g., "btree", "hash"). If NULL, all access methods are included.
- : Optional regex pattern to filter results by input data type name. Matches against both internal type names and external formatted type names. If NULL, all types are included.
- : Boolean flag that controls whether to include additional columns (operator family and owner) in the output.

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
  - printfPQExpBuffer
  - validateSQLNamePattern
  - PSQLexec
  - termPQExpBuffer
  - lengthof
  - printQuery
  - PQExpBufferData
  - printQueryOpt
- Called from (representative examples):
  - exec_command_d (psql command dispatcher)

## Notes and Other Information
- This function is part of psql's describe command family, specifically handling the  command
- Pattern matching follows PostgreSQL's standard SQL pattern syntax with support for wildcards
- The function uses internationalization support through gettext_noop() for column headers
- Error handling includes proper cleanup of allocated buffers on failure paths
- The query uses visibility functions like  to handle schema-qualified names appropriately
- Default operator classes are identified by the  boolean field and displayed as "yes"/"no"