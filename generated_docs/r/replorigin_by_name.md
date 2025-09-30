# replorigin_by_name

## Location
[src/backend/replication/logical/origin.c:221-251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L221-L251)

## Overview
Looks up a replication origin by its name in the system catalog and returns its unique identifier, with optional error handling for missing origins.

## Definition

```c
RepOriginId
replorigin_by_name(const char *roname, bool missing_ok)
```
## Detailed Description
This function searches the pg_replication_origin system catalog for a replication origin with the specified name. It converts the C string name to a PostgreSQL text datum and performs a system cache lookup using the REPLORIGNAME cache. If found, it extracts and returns the origin identifier (roident). The function provides flexible error handling: when missing_ok is false, it throws an error if the origin doesn't exist; when missing_ok is true, it returns InvalidOid for missing origins, allowing callers to handle the absence gracefully.

## Parameters / Member Variables
- : The name of the replication origin to look up (null-terminated C string)
- : Boolean flag controlling error behavior when the origin is not found (true = return InvalidOid, false = throw error)

## Dependencies
- Functions called/Symbols referenced:
  - CStringGetTextDatum
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - Form_pg_replication_origin
  - RepOriginId
- Called from (representative examples):
  - [AlterSubscription](../A/AlterSubscription.md)
  - [ParallelApplyWorkerMain](../P/ParallelApplyWorkerMain.md)
  - [replorigin_drop_by_name](replorigin_drop_by_name.md)
  - [pg_replication_origin_oid](../p/pg_replication_origin_oid.md)
  - [pg_replication_origin_session_setup](../p/pg_replication_origin_session_setup.md)
  - [pg_replication_origin_advance](../p/pg_replication_origin_advance.md)
  - [pg_replication_origin_progress](../p/pg_replication_origin_progress.md)
  - [LogicalRepSyncTableStart](../L/LogicalRepSyncTableStart.md)
  - [run_apply_worker](run_apply_worker.md)
  - [binary_upgrade_replorigin_advance](../b/binary_upgrade_replorigin_advance.md)

## Notes and Other Information
- Returns RepOriginId (which is an Oid type) representing the unique identifier of the replication origin
- Uses the REPLORIGNAME system cache for efficient lookups
- Properly manages system cache resources by calling ReleaseSysCache after use
- When missing_ok is false, throws ERRCODE_UNDEFINED_OBJECT error with a descriptive message
- This function is a core building block for many replication origin operations that need to resolve names to IDs
- Used extensively throughout the logical replication subsystem for origin management

## Simplified Source

```c
RepOriginId
replorigin_by_name(const char *roname, bool missing_ok)
{
    Form_pg_replication_origin ident;
    Oid roident = InvalidOid;
    HeapTuple tuple;
    Datum roname_d;

    // Convert C string to PostgreSQL text datum
    roname_d = CStringGetTextDatum(roname);

    // Look up replication origin by name in system cache
    tuple = SearchSysCache1(REPLORIGNAME, roname_d);
    if (HeapTupleIsValid(tuple))
    {
        // Extract the origin identifier from the tuple
        ident = (Form_pg_replication_origin) GETSTRUCT(tuple);
        roident = ident->roident;
        ReleaseSysCache(tuple);
    }
    else if (!missing_ok)
    {
        // Throw error if origin not found and missing_ok is false
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("replication origin \"%s\" does not exist",
                        roname)));
    }

    return roident;
}
```