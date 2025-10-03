# expand_schema_name_patterns

## Location
[src/bin/pg_dump/pg_dump.c:1449-1507](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L1449-L1507)

## Overview
Finds the OIDs of all schemas matching the given list of patterns and appends them to the provided OID list for use in pg_dump filtering operations.

## Definition

```c
static void
expand_schema_name_patterns(Archive *fout,
							SimpleStringList *patterns,
							SimpleOidList *oids,
							bool strict_names)
```
## Detailed Description
This function processes a list of schema name patterns (which may include wildcards) and resolves them to actual schema OIDs by querying the PostgreSQL system catalog pg_namespace. It supports pattern matching through the processSQL    ePattern function and can handle both simple names and qualified names with database specifications.

The function performs the following operations for each pattern:
1. Constructs a SELECT query against pg_catalog.pg_namespace
2. Processes the pattern using processSQL    ePattern to handle wildcards and special characters
3. Validates that qualified names don't have too many components (database.schema format)
4. Executes the query and collects matching schema OIDs
5. Optionally enforces strict matching (fails if no matches found)

The function accumulates all matching OIDs in the provided oids list, allowing duplicates which are handled by the caller.

## Parameters / Member Variables
- `*fout`: Archive structure containing database connection and dump context
- `*patterns`: SimpleStringList containing schema name patterns to match (may include wildcards)
- `*oids`: SimpleOidList to append matching schema OIDs to
- `strict_names`: Boolean flag that causes failure if any pattern matches no schemas
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md), resetPQExpBuffer, destroyPQExpBuffer (query buffer management)
  - processSQL    ePattern (pattern matching and SQL generation)
  - [GetConnection](../G/GetConnection.md) (database connection retrieval)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md) (query execution)
  - [prohibit_crossdb_refs](../p/prohibit_crossdb_refs.md) (cross-database reference validation)
  - [simple_oid_list_append](../s/simple_oid_list_append.md) (OID list management)
  - atooid (string to OID conversion)
  - [pg_fatal](../p/pg_fatal.md) (error reporting)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dump.c at lines 879, 885)
  - fmtQualifiedDumpable (in pg_dump.c at line 186)

## Notes and Other Information
- The function allows duplicate OIDs in the result list, as duplicates are handled by the caller
- Qualified schema names are validated to prevent improper cross-database references
- Pattern matching supports standard SQL wildcards through the processSQL    ePattern function
- When strict_names is true, the function will terminate with a fatal error if any pattern produces no matches
- The function builds separate queries for each pattern rather than trying to combine them into a single query
- Cross-database references are explicitly prohibited and will cause fatal errors when detected