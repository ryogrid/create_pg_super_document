# collectRoleNames

## Location
src/bin/pg_dump/pg_dump.c: 9982 - 10016

## Overview
Constructs and populates a sorted table of all PostgreSQL roles for efficient lookup during pg_dump operations.

## Definition
```c
static void collectRoleNames(Archive *fout)
```

## Detailed Description
This function initializes the global role name cache used by `getRoleName()` for efficient role name lookups throughout the pg_dump process. It queries the `pg_catalog.pg_roles` system view to retrieve all role OIDs and names, then populates the global `rolenames` array with `RoleNameItem` structures. The query results are ordered by OID, ensuring the resulting array is sorted for binary search operations. This function is called early in the pg_dump process to cache all role information, avoiding repeated database queries when resolving role ownership and privilege information for database objects. The cached data remains valid for the duration of the dump operation.

## Parameters / Member Variables
- `fout`: Archive handle for the pg_dump operation, used for executing the SQL query

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)/PQgetvalue
  - pg_malloc
  - atooid
  - [pg_strdup](../p/pg_strdup.md)
  - [PQclear](../P/PQclear.md)
- Global variables modified:
  - `rolenames`: Array of RoleNameItem structures populated by this function
  - `nrolenames`: Set to the number of roles found
- Called from (representative examples):
  - [main](../m/main.md) (during pg_dump initialization)
  - fmtQualifiedDumpable

## Notes and Other Information
- This is a static function local to pg_dump.c
- The function queries `pg_catalog.pg_roles` which includes all types of roles (users, groups, etc.)
- Results are automatically sorted by OID due to the "ORDER BY 1" clause
- The function allocates memory for the global `rolenames` array that persists for the entire dump
- Essential for performance when dumping databases with many objects that reference roles
- The cached role information includes both login and non-login roles
- Memory allocated by this function is not explicitly freed (relies on process termination cleanup)
- Must be called before any functions that use `getRoleName()`