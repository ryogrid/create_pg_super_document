# listExtendedStats

## Location
[src/bin/psql/describe.c:4694-4789](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L4694-L4789)

## Overview
The  function implements the  psql command for displaying extended statistics objects in a PostgreSQL database.

## Definition

```c
bool
listExtendedStats(const char *pattern)
```
## Detailed Description
This function queries the  system catalog to retrieve and display information about extended statistics objects. Extended statistics are multi-column statistics that help the PostgreSQL query planner make better decisions for complex queries involving correlated columns. The function constructs a SQL query that shows the schema, name, definition, and types of extended statistics (ndistinct, dependencies, and MCV for PostgreSQL 12+).

The function includes version-specific logic to handle differences in PostgreSQL versions:
- Requires PostgreSQL 10.0+ (extended statistics were introduced in version 10)
- Uses different column definition queries for PostgreSQL 14+ vs earlier versions
- Includes MCV (Most Common Values) statistics for PostgreSQL 12+

## Parameters / Member Variables
- `*pattern`: A SQL name pattern (with optional wildcards) to filter which extended statistics to display. If NULL, all visible extended statistics are shown.
## Dependencies
- Functions called/Symbols referenced:
  - : Formats PostgreSQL version numbers for display
  - : Initializes a dynamic string buffer
  - : Adds formatted text to the buffer
  - : Appends formatted text to the buffer
  - : Validates and processes SQL name patterns with wildcards
  - : Executes the constructed SQL query
  - : Formats and displays the query results
  - : Cleans up the string buffer
- Called from (representative examples):
  - : Main dispatcher for psql describe commands

## Notes and Other Information
- The function returns  on success,  on failure
- Displays an error message and returns  (non-fatal) if the server version is too old
- The output includes schema, name, definition, and availability of different statistic types
- Uses internationalization (gettext) for column headers
- Respects object visibility rules through 
- Results are ordered by schema name and object name