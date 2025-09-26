# replorigin_by_name

## Location
src/backend/replication/logical/origin.c: 221 - 251

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
  - SearchSysCache1
  - HeapTupleIsValid
  - GETSTRUCT
  - ReleaseSysCache
  - ereport
  - errcode
  - errmsg
  - Form_pg_replication_origin
  - RepOriginId
- Called from (representative examples):
  - AlterSubscription
  - ParallelApplyWorkerMain
  - replorigin_drop_by_name
  - pg_replication_origin_oid
  - pg_replication_origin_session_setup
  - pg_replication_origin_advance
  - pg_replication_origin_progress
  - LogicalRepSyncTableStart
  - run_apply_worker
  - binary_upgrade_replorigin_advance

## Notes and Other Information
- Returns RepOriginId (which is an Oid type) representing the unique identifier of the replication origin
- Uses the REPLORIGNAME system cache for efficient lookups
- Properly manages system cache resources by calling ReleaseSysCache after use
- When missing_ok is false, throws ERRCODE_UNDEFINED_OBJECT error with a descriptive message
- This function is a core building block for many replication origin operations that need to resolve names to IDs
- Used extensively throughout the logical replication subsystem for origin management