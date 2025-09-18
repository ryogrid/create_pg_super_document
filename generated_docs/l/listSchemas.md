# listSchemas

## Location
[src/bin/psql/describe.c:5026-5146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5026-L5146)

## Overview
The  function implements the  psql command for displaying schema (namespace) information in a PostgreSQL database.

## Definition


## Detailed Description
This function queries the  system catalog to retrieve and display information about schemas defined in the database. Schemas are logical containers that organize database objects like tables, views, functions, and types. The function shows schema names, owners, and optionally access control lists (ACLs) and descriptions.

The function includes special handling for PostgreSQL 15+ to show publication information when a specific schema pattern is provided. It queries the publication system catalogs to display which publications include the schema, providing useful information for logical replication setups.

The query can optionally exclude system schemas (those starting with 'pg_' and 'information_schema') and supports pattern matching for schema names.

## Parameters / Member Variables
- : A SQL name pattern (with optional wildcards) to filter which schemas to display. If NULL, all visible schemas are shown. When a specific pattern is provided in PostgreSQL 15+, publication information is also retrieved.
- : If true, includes access control lists (permissions) and schema descriptions from the  catalog in the output.
- : If true, includes system schemas ('pg_*' and 'information_schema'); if false, excludes them (unless a pattern is specified).

## Dependencies
- Functions called/Symbols referenced:
  - : Initializes a dynamic string buffer
  - : Adds formatted text to the buffer
  - : Appends formatted text to the buffer
  - : Formats access control list (ACL) information for display
  - : Validates and processes SQL name patterns with wildcards
  - : Executes the constructed SQL query
  - : Formats and displays the query results
  - : Cleans up the string buffer
  - : Allocates memory for footer strings
  - : Frees allocated memory
  - : Gets the number of result rows
  - : Gets a specific field value from the result
- Called from (representative examples):
  - : Main dispatcher for psql describe commands

## Notes and Other Information
- The function returns  on success,  on failure
- Uses error handling with goto for cleanup on validation failures
- System schema filtering logic: excludes schemas matching '^pg_' regex pattern and 'information_schema'
- For PostgreSQL 15+, when a specific pattern is provided, displays publication information as footers
- [Publication](../P/Publication.md) footer shows which publications include the matched schema for logical replication
- Memory management includes proper cleanup of dynamically allocated footer strings
- Results are ordered by schema name
- ACL information shows permissions granted to different roles when verbose mode is enabled
- The function handles both single schema queries (with publications) and general schema listings