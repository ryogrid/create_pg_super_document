# systable_endscan_ordered

## Location
[src/backend/access/index/genam.c:745-794](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/genam.c#L745-L794)

## Overview
Terminates an ordered system catalog scan and properly releases all associated resources including tuple slots, index scans, and snapshots.

## Definition
void systable_endscan_ordered(SysScanDesc sysscan)

## Detailed Description
This function serves as the cleanup counterpart to systable_beginscan_ordered(), responsible for properly terminating an ordered catalog scan and releasing all allocated resources. The function performs several critical cleanup operations: drops the tuple table slot used for storing scan results, ends the underlying index scan, unregisters any snapshot that was used for the scan, and manages transaction-level scan tracking flags.

A key aspect of this function is its handling of the bsysscan flag, which is used to track whether bootstrap catalog scans are in progress. This flag is reset when certain transaction conditions are met, helping to coordinate catalog access during system initialization and recovery scenarios.

## Parameters / Member Variables
- `sysscan`: SysScanDesc descriptor containing all scan state that needs to be cleaned up, including the tuple slot, index scan, and snapshot

## Dependencies
- Functions called/Symbols referenced:
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
  - [index_endscan](../i/index_endscan.md)
  - [UnregisterSnapshot](../U/UnregisterSnapshot.md)
  - TransactionIdIsValid
  - CheckXidAlive (global variable)
  - [pfree](../p/pfree.md)
  - [SysScanDesc](../S/SysScanDesc.md) (type)
- Called from (representative examples):
  - [toast_delete_datum](../t/toast_delete_datum.md)
  - [heap_fetch_toast_slice](../h/heap_fetch_toast_slice.md)
  - [inv_getsize](../i/inv_getsize.md)
  - [inv_read](../i/inv_read.md)
  - [inv_write](../i/inv_write.md)
  - [enum_endpoint](../e/enum_endpoint.md)
  - [BuildEventTriggerCache](../B/BuildEventTriggerCache.md)

## Notes and Other Information
- Must be called for every successful systable_beginscan_ordered() to prevent resource leaks
- Safely handles NULL slot pointers by checking before attempting to drop
- Requires that sysscan->irel is non-NULL (active index relation)
- Manages the global bsysscan flag used for bootstrap scan coordination
- The sysscan descriptor itself is freed, so it cannot be used after this call
- Part of the ordered scanning API that provides proper resource management for index-based catalog scans

## Simplified Source

```c
void systable_endscan_ordered(SysScanDesc sysscan)
{
    // Clean up tuple slot if it exists
    if (sysscan->slot)
    {
        ExecDropSingleTupleTableSlot(sysscan->slot);
        sysscan->slot = NULL;
    }

    // End the index scan (index relation must exist)
    Assert(sysscan->irel);
    index_endscan(sysscan->iscan);

    // Unregister snapshot if we registered one
    if (sysscan->snapshot)
        UnregisterSnapshot(sysscan->snapshot);

    // Reset bootstrap scan flag if transaction monitoring is active
    if (TransactionIdIsValid(CheckXidAlive))
        bsysscan = false;

    // Free the scan descriptor
    pfree(sysscan);
}
```