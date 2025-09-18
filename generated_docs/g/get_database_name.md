# get_database_name

## Location
[src/backend/commands/dbcommands.c:3174-3197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L3174-L3197)

## Overview
A utility function that retrieves the database name given its OID (Object Identifier) from the PostgreSQL system catalog.

## Definition


## Detailed Description
This function looks up a database name in the system catalog pg_database using the provided database OID. It performs a system cache lookup for efficiency, returning a palloc'd string containing the database name if found, or NULL if no database exists with the given OID. The function is widely used throughout PostgreSQL for converting database OIDs to human-readable names in logging, error messages, and various administrative operations.

## Parameters / Member Variables
- : The Object Identifier (Oid) of the database whose name is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - [pstrdup](../p/pstrdup.md) (string duplication with palloc)
  - NameStr (name extraction macro)
  - GETSTRUCT (tuple structure access macro)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_database (catalog structure type)
- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md) (vacuum operations)
  - [GetNewMultiXactId](../G/GetNewMultiXactId.md) (transaction management)
  - [createdb](../c/createdb.md) (database creation)
  - [do_autovacuum](../d/do_autovacuum.md) (automatic maintenance)
  - [current_database](../c/current_database.md) (SQL function)

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller
- Returns NULL if the database OID does not exist in the system catalog
- Uses system cache for performance optimization
- Thread-safe and can be called from any backend process
- Commonly used in logging and error reporting throughout the PostgreSQL codebase