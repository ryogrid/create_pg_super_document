# objectDescription

## Location
[src/bin/psql/describe.c:1252-1444](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L1252-L1444)

## Overview
Implements the \dd command in psql to display comments for database objects that don't have their own dedicated describe commands.

## Definition

```c
bool
objectDescription(const char *pattern, bool showSystem)
```
## Detailed Description
The  function implements the psql \dd command, which lists comments for specific types of database objects. Unlike other describe commands that show comprehensive object information, this function focuses solely on retrieving and displaying user-defined comments/descriptions for objects.

The function specifically handles these object types:
- Table constraints (check, foreign key, unique, etc.)
- Domain constraints  
- Operator classes
- Operator families
- Rules (excluding view rules)
- Triggers

It constructs a complex SQL query that unions together queries for each object type, retrieving the schema name, object name, object type, and description from the PostgreSQL system catalogs. The results are formatted and displayed in a tabular format.

## Parameters / Member Variables
- `*pattern`: SQL pattern to filter object names (supports wildcards like *, ?, etc.). If NULL, shows all objects.
- `showSystem`: Boolean flag to include system objects (pg_catalog, information_schema). If false, only user objects are shown.
## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md): Initialize query buffer
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md): Validate and process name patterns for SQL queries
  - [PSQLexec](../P/PSQLexec.md): Execute the constructed SQL query
  - [printQuery](../p/printQuery.md): Format and display query results
  - [termPQExpBuffer](../t/termPQExpBuffer.md): Clean up query buffer
  - lengthof: Get array length
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md): Main command dispatcher for \dd command in psql

## Notes and Other Information
- The function uses a UNION ALL approach to combine results from multiple system catalog queries
- System objects are filtered out by default unless showSystem is true
- Only objects with actual comments (entries in pg_description) are displayed
- The function handles internationalization through gettext_noop for column headers
- Error handling includes proper cleanup of allocated buffers on failure
- Results are ordered by schema, name, and object type for consistent display