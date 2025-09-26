# replorigin_create

## Location
src/backend/replication/logical/origin.c: 252 - 340

## Overview
Creates a new replication origin with the specified name by finding an unused 16-bit identifier and inserting a new row into the pg_replication_origin system catalog.

## Definition


## Detailed Description
This function creates a new replication origin by performing a sequential search for an unused 16-bit origin identifier and inserting a corresponding catalog entry. The process uses an exclusive table lock to prevent concurrency issues while ensuring that newly created origins are immediately visible through dirty snapshot reads. The function enforces that replication origin IDs fit within 16-bit range (PG_UINT16_MAX) and uses a systematic approach to find the first available ID starting from InvalidOid + 1. The implementation prioritizes correctness over efficiency, as replication origin creation is expected to be an infrequent operation.

## Parameters / Member Variables
- : The name of the replication origin to create (null-terminated C string)

## Dependencies
- Functions called/Symbols referenced:
  - CStringGetTextDatum
  - IsTransactionState
  - InitDirtySnapshot
  - table_open
  - table_close
  - ScanKeyInit
  - systable_beginscan
  - systable_getnext
  - systable_endscan
  - HeapTupleIsValid
  - heap_form_tuple
  - CatalogTupleInsert
  - CommandCounterIncrement
  - heap_freetuple
  - ereport
  - errcode
  - errmsg
  - CHECK_FOR_INTERRUPTS
  - ObjectIdGetDatum
  - RelationGetDescr
  - SnapshotData
  - SysScanDesc
  - ExclusiveLock
  - PG_UINT16_MAX
- Called from (representative examples):
  - CreateSubscription
  - pg_replication_origin_create
  - LogicalRepSyncTableStart
  - run_apply_worker

## Notes and Other Information
- Must be called within a transaction context (asserted via IsTransactionState)
- Uses exclusive table locking to handle concurrency during ID allocation
- Employs dirty snapshots to see uncommitted changes from other transactions
- Searches sequentially for unused IDs from 1 to PG_UINT16_MAX (65535)
- Throws ERRCODE_PROGRAM_LIMIT_EXCEEDED if no free ID is available
- Calls CommandCounterIncrement to make the new origin immediately visible to subsequent operations
- The 16-bit ID limitation is a deliberate design choice for efficient storage and operations
- Returns the newly allocated RepOriginId on success