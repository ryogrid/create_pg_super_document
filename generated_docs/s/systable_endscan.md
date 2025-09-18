# systable_endscan

## Location
src/backend/access/index/genam.c: 598 - 644

## Overview
systable_endscan is a cleanup function that closes a PostgreSQL system catalog scan, releasing all associated resources including tuple slots, index scans, and snapshots.

## Definition
```c
void systable_endscan(SysScanDesc sysscan)
```

## Detailed Description
This function provides proper cleanup and resource deallocation for system catalog scans initiated by systable_beginscan functions. It handles cleanup for both index-based and heap-based scans by checking the scan descriptor's state and calling appropriate cleanup functions.

The function performs several cleanup operations in sequence: first it drops the tuple table slot if one exists, then it ends either the index scan or table scan depending on which was used, unregisters any snapshots that were registered during the scan, resets the bsysscan flag when CheckXidAlive transactions are being monitored, and finally frees the scan descriptor memory.

The bsysscan flag reset is specifically related to transaction monitoring during logical replication scenarios, where CheckXidAlive tracks transactions that need concurrent abort detection.

## Parameters / Member Variables
- : A SysScanDesc structure containing the scan state to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md) (slot cleanup)
  - [index_endscan](../i/index_endscan.md) (index scan cleanup)
  - [index_close](../i/index_close.md) (index relation cleanup)
  - [table_endscan](../t/table_endscan.md) (heap scan cleanup)
  - UnregisterSnapshot (snapshot cleanup)
  - TransactionIdIsValid (transaction validation)
  - [pfree](../p/pfree.md) (memory deallocation)
- Called from (representative examples):
  - Extensively used throughout PostgreSQL codebase wherever systable_beginscan is used
  - Found in over 20 files including utils/cache, commands, replication, and catalog modules

## Notes and Other Information
- The caller is still responsible for closing the heap relation after calling this function
- This function does not return any value (void)
- Properly handles both index-based and sequential scan cleanup paths
- Part of the paired beginscan/endscan API for system catalog access
- Critical for preventing memory leaks and resource exhaustion in catalog scanning operations
- The bsysscan flag management is related to logical replication's transaction monitoring system