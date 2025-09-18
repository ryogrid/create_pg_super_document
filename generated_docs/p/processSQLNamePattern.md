# processSQLNamePattern

## Location
[src/fe_utils/string_utils.c:1053-1063](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/string_utils.c#L1053-L1063)

## Overview
Processes wildcard-pattern strings and generates appropriate WHERE clauses to limit database object queries, supporting schema-qualified names with visibility constraints.

## Definition


## Detailed Description
This function is a core utility in PostgreSQL's frontend tools for handling user-specified patterns when querying database objects. It converts shell-style patterns (with wildcards like * and ?) into SQL WHERE clauses using regular expression matching.

The function handles several key responsibilities:
- Converts shell-style patterns to SQL regular expressions using 
- Generates WHERE clauses for schema and/or name pattern matching
- Handles visibility rules to restrict results to accessible objects
- Supports alternative name variables for objects with multiple naming schemes
- Optimizes away trivial "*" patterns that match everything
- Ensures proper SQL escaping and collation handling for PostgreSQL v12+

The function is designed to work with hostile search paths by fully qualifying all names and using explicit COLLATE clauses when needed for proper character matching behavior.

## Parameters / Member Variables
- : PostgreSQL connection handle used for escaping rules and server version detection
- : Output buffer where generated WHERE clauses are appended
- : User-specified pattern string, or NULL for default "*" (match all)
- : True if caller already emitted "WHERE" keyword (clauses will be ANDed)
- : Always quote regexp special characters, even outside double quotes
- : Name of query variable to match against schema pattern (can be NULL)
- : Name of query variable to match against object name pattern
- : Alternative variable name for objects with multiple names (can be NULL)
- : Clause to restrict to visible objects (e.g., "pg_catalog.pg_table_is_visible(p.oid)")
- : Output buffer for database name portion of pattern (can be NULL)
- : Output parameter for number of separators parsed from pattern

## Dependencies
- Functions called/Symbols referenced:
  -  - Converts shell patterns to SQL regex
  -  - Buffer data structure
  -  - [Initialize](../I/Initialize.md) buffer
  -  - Clean up buffer
  -  - Append formatted data to buffer
  -  - Append string to buffer
  -  - Append character to buffer
  -  - Append escaped string literal
  -  - Get client encoding
  -  - Get server version

- Called from (representative examples):
  -  in pg_dump.c:1477
  -  in pg_dump.c:1533
  -  in pg_dump.c:1661
  -  in psql describe.c:4581
  -  in psql describe.c:6175

## Notes and Other Information
- The function uses a local WHEREAND() macro to manage WHERE/AND clause generation
- For PostgreSQL v12+, explicit COLLATE pg_catalog.default clauses are added to ensure proper regex matching behavior
- The function handles optimization by skipping trivial "^(.*)$" patterns that would match everything
- When no schema pattern is given, visibility rules are automatically applied to show only accessible objects
- Buffer management is handled internally with proper cleanup of temporary buffers
- The function is located in src/fe_utils/string_utils.c:1053-1174 and is part of PostgreSQL's frontend utilities