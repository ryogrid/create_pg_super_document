# errtablecolname

## Location
src/backend/utils/cache/relcache.c: 5998 - 6010

## Overview
Stores schema name, table name, and column name of a table column within the current error context, where the column name is provided directly.

## Definition
```c
int errtablecolname(Relation rel, const char *colname)
```

## Detailed Description
This function enhances error reporting by capturing table and column-specific context information and storing it in the current error data structure. Unlike errtablecol(), this function accepts the column name directly rather than resolving it from an attribute number. It first calls errtable() to store the schema and table information, then adds the column name to the error context using PostgreSQL's error reporting framework.

This function is primarily used by errtablecol() internally, but can also be used directly when the column name is already known and errtablecol() would be inconvenient. It's particularly useful during intermediate states in operations like ALTER TABLE where the relation's catalog data might be in an inconsistent state.

## Parameters / Member Variables
- `rel`: The relation (table) containing the column
- `colname`: The name of the column to include in error context

## Dependencies
- Functions called/Symbols referenced:
  - [errtable](errtable.md)
  - [err_generic_string](err_generic_string.md) (with PG_DIAG_COLUMN_NAME)
- Called from (representative examples):
  - [errtablecol](errtablecol.md)

## Notes and Other Information
- Lower-level function primarily used by errtablecol(), but available for direct use when needed
- Useful during intermediate states in ALTER TABLE operations where catalog data might be inconsistent
- Part of PostgreSQL's structured error reporting system for enhanced debugging
- The return value (0) does not matter and is ignored by callers
- Builds upon errtable() to provide complete table and column context in error messages