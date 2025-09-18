# expand_extension_name_patterns

## Location
src/bin/pg_dump/pg_dump.c: 1508 - 1560

## Overview
Finds the OIDs of all extensions matching the given list of patterns and appends them to the provided OID list for use in pg_dump filtering operations.

## Definition


## Detailed Description
This function processes a list of extension name patterns (which may include wildcards) and resolves them to actual extension OIDs by querying the PostgreSQL system catalog pg_extension. It is similar to expand_schema_name_patterns but specifically handles PostgreSQL extensions.

The function performs the following operations for each pattern:
1. Constructs a SELECT query against pg_catalog.pg_extension
2. Processes the pattern using processSQL    ePattern to handle wildcards and special characters
3. Validates that the pattern is not a qualified name (extensions don't support qualification)
4. Executes the query and collects matching extension OIDs
5. Optionally enforces strict matching (fails if no matches found)

Unlike schema patterns, extension patterns cannot be qualified names - any pattern containing dots will cause a fatal error since extensions are not schema-qualified objects.

## Parameters / Member Variables
- : Archive structure containing database connection and dump context
- : SimpleStringList containing extension name patterns to match (may include wildcards)
- : SimpleOidList to append matching extension OIDs to
- : Boolean flag that causes failure if any pattern matches no extensions

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer, resetPQExpBuffer, destroyPQExpBuffer (query buffer management)
  - processSQL    ePattern (pattern matching and SQL generation)
  - [GetConnection](../G/GetConnection.md) (database connection retrieval)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md) (query execution)
  - [simple_oid_list_append](../s/simple_oid_list_append.md) (OID list management)
  - atooid (string to OID conversion)
  - [pg_fatal](../p/pg_fatal.md) (error reporting)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dump.c at lines 924, 930)
  - fmtQualifiedDumpable (in pg_dump.c at line 190)

## Notes and Other Information
- Extension names cannot be qualified, so any pattern containing dots (dotcnt > 0) results in a fatal error
- The function allows duplicate OIDs in the result list, as duplicates are handled by the caller
- Pattern matching supports standard SQL wildcards through the processSQL    ePattern function
- When strict_names is true, the function will terminate with a fatal error if any pattern produces no matches
- Extensions are database-wide objects and are not contained within schemas, which is why qualification is not allowed
- The function builds separate queries for each pattern rather than trying to combine them into a single query