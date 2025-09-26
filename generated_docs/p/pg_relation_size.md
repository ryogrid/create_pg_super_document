# pg_relation_size

## Location
[src/backend/utils/adt/dbsize.c:346-377](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/dbsize.c#L346-L377)

## Overview
A PostgreSQL system function that returns the disk space used by a specific fork of a relation identified by its OID.

## Definition
```c
Datum pg_relation_size(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a SQL-callable interface to determine the physical size of a specific fork of a relation (table, index, etc.). It takes a relation OID and a fork name as parameters, opens the relation with an AccessShareLock, and delegates the actual size calculation to `calculate_relation_size`. 

The function includes robust error handling that was improved in PostgreSQL 9.2 - instead of throwing errors for dropped relations, it returns NULL. This design makes queries like "SELECT pg_relation_size(oid) FROM pg_class" more reliable, as they won't abort if a relation is dropped by another session during the scan.

The function handles different relation forks (main data, free space map, visibility map, etc.) based on the fork name parameter, making it a versatile tool for analyzing storage usage at a granular level.

## Parameters / Member Variables
- `relOid`: OID of the relation whose size is to be calculated
- `forkName`: Text parameter specifying the fork name (e.g., 'main', 'fsm', 'vm', 'init')

## Dependencies
- Functions called/Symbols referenced:
  - [try_relation_open](../t/try_relation_open.md)
  - [calculate_relation_size](../c/calculate_relation_size.md)
  - [text_to_cstring](../t/text_to_cstring.md)
  - [forkname_to_number](../f/forkname_to_number.md)
  - [relation_close](../r/relation_close.md)
  - PG_GETARG_OID
  - PG_GETARG_TEXT_PP
  - PG_RETURN_NULL
  - PG_RETURN_INT64
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL interface)

## Dependencies
- Functions called/Symbols referenced:
  - [try_relation_open](../t/try_relation_open.md)
  - [calculate_relation_size](../c/calculate_relation_size.md)
  - [text_to_cstring](../t/text_to_cstring.md)
  - [forkname_to_number](../f/forkname_to_number.md)
  - [relation_close](../r/relation_close.md)
  - PG_GETARG_OID
  - PG_GETARG_TEXT_PP
  - PG_RETURN_NULL
  - PG_RETURN_INT64
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL interface)

## Notes and Other Information
- This function is part of PostgreSQL's system administration functions accessible via SQL
- Uses `try_relation_open` instead of `relation_open` to avoid errors on non-existent relations
- Returns NULL for dropped relations instead of throwing errors (behavior changed in PostgreSQL 9.2)
- Acquires AccessShareLock on the relation during size calculation to ensure consistency
- Supports all relation fork types through the forkname parameter
- The function is defined in src/backend/utils/adt/dbsize.c:346-377
- Commonly used in monitoring and administrative queries to analyze storage usage patterns
- The fork name parameter allows for detailed analysis of different components of relation storage