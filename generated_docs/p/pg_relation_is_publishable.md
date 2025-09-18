# pg_relation_is_publishable

## Location
src/backend/catalog/pg_publication.c: 163 - 181

## Overview
A SQL-callable function that determines if a relation is publishable based on its OID, designed to be safe for use in tools like psql where concurrent catalog changes may occur.

## Definition
```c
Datum pg_relation_is_publishable(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a SQL-accessible interface to the publishability checking logic. It differs from the internal `is_publishable_relation()` and `is_publishable_class()` functions in several important ways:

1. **SQL Interface**: Uses PostgreSQL's function manager (fmgr) interface to be callable from SQL
2. **Graceful Error Handling**: Returns NULL when the relation doesn't exist, rather than throwing errors
3. **Concurrency Safe**: Designed for use in interactive tools where relations might be dropped between discovery and checking

The function:
- Takes a relation OID as input via PG_GETARG_OID(0)
- Looks up the relation in the system catalog using SearchSysCache1()
- Returns NULL if the relation doesn't exist (graceful degradation)
- Delegates actual publishability logic to `is_publishable_class()`
- Properly manages system cache lifecycle with ReleaseSysCache()

This design makes it suitable for use in administrative tools and interactive sessions where catalog objects might be concurrently modified.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function manager arguments, containing:
  - Argument 0: Oid of the relation to check

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (macro to extract OID argument)
  - [SearchSysCache1](../S/SearchSysCache1.md) (system catalog lookup)
  - HeapTupleIsValid (tuple validity check)
  - [is_publishable_class](../i/is_publishable_class.md) (core publishability logic)
  - GETSTRUCT (macro to extract struct from tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - PG_RETURN_NULL (return NULL to SQL)
  - PG_RETURN_BOOL (return boolean to SQL)
- Types used:
  - Form_pg_class
  - HeapTuple
- Called from:
  - SQL queries (no direct C code references found)

## Notes and Other Information
- This is a PostgreSQL built-in function accessible from SQL
- Designed specifically for tools like psql where graceful error handling is important
- Uses the system cache interface for efficient catalog lookups
- Returns PostgreSQL Datum type for SQL interoperability
- The NULL return behavior prevents errors in concurrent environments
- Function likely registered in pg_proc.dat for SQL accessibility
- Part of PostgreSQL's administrative/introspection function suite
- Location: src/backend/catalog/pg_publication.c:163-181