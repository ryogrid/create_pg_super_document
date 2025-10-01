# StatisticsGetRelation

## Location
[src/backend/commands/statscmds.c:898-917](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/statscmds.c#L898-L917)

## Overview
Retrieves the OID of the relation (table) that a given extended statistics object is defined on by looking up the statistics object in the system catalog.

## Definition
```c
Oid
StatisticsGetRelation(Oid statId, bool missing_ok)
```

## Detailed Description
This function performs a system cache lookup to find the relation OID associated with a specific extended statistics object. It searches the pg_statistic_ext system catalog using the statistics object OID and extracts the stxrelid field, which contains the OID of the table or relation that the statistics object belongs to. The function provides error handling options through the missing_ok parameter, allowing callers to choose between receiving an error or InvalidOid when the statistics object is not found.

The function uses PostgreSQL system cache for efficient lookups and includes proper cache management by releasing the tuple after extracting the needed information.

## Parameters / Member Variables
- `statId`: OID of the extended statistics object to look up
- `missing_ok`: If true, return InvalidOid when statistics object is not found; if false, throw an error

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](SearchSysCache1.md)
  - HeapTupleIsValid
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_statistic_ext (struct type)
- Called from (representative examples):
  - [ATPostAlterTypeCleanup](../A/ATPostAlterTypeCleanup.md)
  - Various functions via DEFREM_H header inclusion

## Notes and Other Information
- Uses STATEXTOID cache for efficient lookups in pg_statistic_ext catalog
- Includes assertion to verify the returned tuple matches the requested statId
- Error message includes the statistics object OID for debugging purposes
- Returns InvalidOid when missing_ok is true and statistics object not found
- Part of PostgreSQL extended statistics management infrastructure
- Located in src/backend/commands/statscmds.c (lines 898-917)

## Simplified Source

```c
Oid StatisticsGetRelation(Oid statId, bool missing_ok) {
    HeapTuple tuple;
    Form_pg_statistic_ext stx;
    Oid result;

    // Look up statistics object in system cache
    tuple = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(statId));

    // Handle not found case
    if (!HeapTupleIsValid(tuple)) {
        if (missing_ok)
            return InvalidOid;
        elog(ERROR, "cache lookup failed for statistics object %u", statId);
    }

    // Extract relation OID from statistics object
    stx = (Form_pg_statistic_ext) GETSTRUCT(tuple);
    Assert(stx->oid == statId);
    result = stx->stxrelid;

    // Clean up cache reference
    ReleaseSysCache(tuple);
    return result;
}
```