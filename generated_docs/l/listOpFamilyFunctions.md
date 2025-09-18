# listOpFamilyFunctions

## Location
src/bin/psql/describe.c: 6965 - 7053

## Overview
The  function implements the  psql command to display a formatted list of support functions belonging to operator families, with optional filtering by index access method and operator family name.

## Definition


## Detailed Description
This function constructs and executes an SQL query to retrieve support function information from PostgreSQL system catalogs, specifically from the  table which stores access method support procedures. It displays support functions with their associated access methods, operator family names, registered left and right operand types, procedure numbers, and function names or signatures. The function supports two output modes: non-verbose shows just the function name, while verbose mode shows the complete function signature in regprocedure format. Pattern matching is supported for filtering results by access method name and operator family name.

The query joins multiple system catalogs (, , , , ) to gather comprehensive information about support functions within operator families. Results are sorted by access method, operator family, type compatibility (self-types first), registered types, and procedure number for consistent and meaningful presentation.

## Parameters / Member Variables
- : Optional regex pattern to filter results by index access method name (e.g., "btree", "hash"). If NULL, all access methods are included.
- : Optional regex pattern to filter results by operator family name. If NULL, support functions from all families are included.
- : Boolean flag that controls the function display format. If false, shows just the function name (). If true, shows the complete function signature in regprocedure format ().

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
- Support functions are essential components of operator families that implement the actual logic for index operations
- Procedure numbers indicate the specific role of each support function within the access method (e.g., 1 = comparison function, 2 = hash function, etc.)
- Registered left and right types show which operand type combinations the support function handles
- The sort order prioritizes functions that work with the same left and right types (self-types) first
- In verbose mode, the regprocedure format includes the function name along with its complete argument type signature
- Support functions are critical for the proper functioning of indexes using the associated operator families