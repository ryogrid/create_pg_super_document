# try_table_open

## Location
src/backend/access/table/table.c: 60 - 82

## Overview
Opens a table relation by its object identifier (OID) with graceful error handling, returning NULL instead of raising an error if the relation does not exist.

## Definition
```c
Relation try_table_open(Oid relationId, LOCKMODE lockmode);
```

## Detailed Description
`try_table_open` is a non-throwing variant of `table_open` that provides graceful handling of missing relations. It performs the following actions:
1. Attempts to open the relation using the provided OID and lock mode via `try_relation_open`
2. Returns NULL immediately if the relation does not exist
3. If the relation exists, validates that it is not an index or composite type using `validate_relation_kind`
4. Returns the opened relation if validation passes

This function is useful in scenarios where the existence of a table is uncertain and the caller wants to handle missing tables gracefully rather than catching exceptions.

## Parameters / Member Variables
- `relationId`: Object identifier (OID) of the relation to open
- `lockmode`: Type of lock to acquire on the relation (e.g., AccessShareLock, RowExclusiveLock)

## Dependencies
- Functions called/Symbols referenced:
  - try_relation_open
  - validate_relation_kind
- Called from (representative examples):
  - Functions that need to check table existence conditionally
  - Operations that should continue gracefully if a table is missing

## Notes and Other Information
- Returns NULL if the relation does not exist, unlike `table_open` which raises an error
- Still validates relation type after opening, so will raise an error if the relation is an index or composite type
- Provides the same table-specific validation as `table_open` but with graceful missing-relation handling
- Part of the table access method interface for safe table operations