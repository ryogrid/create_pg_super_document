# get_database_name

## Location
src/backend/commands/dbcommands.c: 3174 - 3197

## Overview
A utility function that retrieves the database name given its OID (Object Identifier) from the PostgreSQL system catalog.

## Definition


## Detailed Description
This function looks up a database name in the system catalog pg_database using the provided database OID. It performs a system cache lookup for efficiency, returning a palloc'd string containing the database name if found, or NULL if no database exists with the given OID. The function is widely used throughout PostgreSQL for converting database OIDs to human-readable names in logging, error messages, and various administrative operations.

## Parameters / Member Variables
- : The Object Identifier (Oid) of the database whose name is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1 (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - pstrdup (string duplication with palloc)
  - NameStr (name extraction macro)
  - GETSTRUCT (tuple structure access macro)
  - ReleaseSysCache (cache cleanup)
  - Form_pg_database (catalog structure type)
- Called from (representative examples):
  - heap_vacuum_rel (vacuum operations)
  - GetNewMultiXactId (transaction management)
  - createdb (database creation)
  - do_autovacuum (automatic maintenance)
  - current_database (SQL function)

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller
- Returns NULL if the database OID does not exist in the system catalog
- Uses system cache for performance optimization
- Thread-safe and can be called from any backend process
- Commonly used in logging and error reporting throughout the PostgreSQL codebase