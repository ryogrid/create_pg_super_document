# describeTypes

## Location
src/bin/psql/describe.c: 615 - 719

## Overview
Implements the \dT psql command to display a comprehensive list of data types in the database, with sophisticated filtering to exclude unwanted complex types and array types.

## Definition
```c
bool describeTypes(const char *pattern, bool verbose, bool showSystem)
```

## Detailed Description
This function generates and executes a SQL query to list data types from the pg_type system catalog. It implements intelligent filtering logic to exclude complex types (unless they are standalone composite types) and array types (unless explicitly requested with '[]' in the pattern). The function constructs queries that show both internal and formatted type names, with verbose mode providing additional details including internal names, sizes, enum elements, ownership, ACL information, and descriptions. It supports pattern matching against both internal type names and formatted type displays.

## Parameters / Member Variables
- `pattern`: Optional regular expression pattern to filter types by name (supports '[]' to include array types)
- `verbose`: Boolean flag to include additional columns (internal name, size, elements, owner, ACL)
- `showSystem`: Boolean flag to control whether system schema types are displayed

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [printACLColumn](../p/printACLColumn.md)
  - [map_typename_pattern](../m/map_typename_pattern.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - termPQExpBuffer
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - [PQclear](../P/PQclear.md)
  - CppAsString2
  - RELKIND_COMPOSITE_TYPE
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (in command.c:923)

## Notes and Other Information
- Part of psql's describe functionality (\dT command)
- Implements sophisticated type filtering logic to provide clean, useful type listings
- Excludes complex types (typrelid!=0) unless they are standalone composite types
- Excludes array types by default unless pattern contains '[]' 
- In verbose mode, shows enum elements with proper sorting (enumsortorder)
- Uses format_type() for user-friendly type name display
- Matches patterns against both internal names (typname) and formatted names
- Shows type size information with special handling for variable-length ('var') and tuple types
- Provides access control information and ownership details in verbose mode
- Uses map_typename_pattern() for enhanced pattern matching capabilities
- Orders results by schema and type name for consistent output