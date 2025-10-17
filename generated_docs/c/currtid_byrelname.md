# currtid_byrelname

## Location
[src/backend/utils/adt/tid.c:408-425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tid.c#L408-L425)

## Overview
A PostgreSQL built-in function that retrieves the latest tuple version of a tuple identified by a CTID for a relation specified by name.

## Definition
```c
Datum currtid_byrelname(PG_FUNCTION_ARGS)
```

## Detailed Description
The `currtid_byrelname` function is a PostgreSQL built-in function (callable from SQL) that provides a user interface for obtaining the current tuple identifier of a specific tuple within a named relation. It serves as a wrapper around the internal CTID handling mechanisms, making this functionality accessible to SQL queries and applications.

The function takes a relation name (as text) and a tuple identifier as input parameters, resolves the relation name to an actual relation object, and then delegates the CTID lookup to the internal `currtid_internal` function. This design allows users to query the latest version of a tuple by name without needing direct access to internal PostgreSQL structures.

The function handles the complete lifecycle of the operation including:
- Parsing the relation name into a qualified name list
- Opening the relation with appropriate locking
- Performing the CTID lookup
- Properly closing the relation and releasing locks
- Returning the result in the appropriate PostgreSQL Datum format

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `relname` (text): Name of the relation, which can be schema-qualified
  - `tid` (ItemPointer): The tuple identifier for which to find the latest version

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP
  - PG_GETARG_ITEMPOINTER
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md)
  - [textToQualifiedNameList](../t/textToQualifiedNameList.md)
  - [table_openrv](../t/table_openrv.md)
  - [currtid_internal](currtid_internal.md)
  - [table_close](../t/table_close.md)
  - PG_RETURN_ITEMPOINTER
- Called from (representative examples):
  - SQL queries and applications (no direct C code references found)

## Notes and Other Information
- This is a public PostgreSQL built-in function accessible from SQL
- The function signature follows PostgreSQL's function call convention using PG_FUNCTION_ARGS
- [Relation](../R/Relation.md) name parsing supports schema-qualified names (e.g., "schema.table")
- The function uses AccessShareLock for safe concurrent access to the relation
- Proper resource management ensures relations are closed and locks released even in error cases
- The function is part of PostgreSQL's system function catalog and can be called directly from SQL queries
- Error handling is delegated to the underlying functions, particularly `currtid_internal` which performs access control checks
- The return type is ItemPointer (TID), which represents the latest version of the requested tuple

## Simplified Source

```c
Datum
currtid_byrelname(PG_FUNCTION_ARGS)
{
    // Extract arguments: relation name and TID
    text *relname = PG_GETARG_TEXT_PP(0);
    ItemPointer tid = PG_GETARG_ITEMPOINTER(1);

    // Parse relation name and open the relation
    RangeVar *relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
    Relation rel = table_openrv(relrv, AccessShareLock);

    // Get the latest tuple version for this TID
    ItemPointer result = currtid_internal(rel, tid);

    // Clean up and return result
    table_close(rel, AccessShareLock);
    PG_RETURN_ITEMPOINTER(result);
}
```