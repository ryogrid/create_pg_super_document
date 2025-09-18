# systable_getnext_ordered

## Location
src/backend/access/index/genam.c: 720 - 744

## Overview
Retrieves the next tuple in an ordered system catalog scan, supporting bidirectional iteration through index-ordered results.

## Definition
HeapTuple systable_getnext_ordered(SysScanDesc sysscan, ScanDirection direction)

## Detailed Description
This function extends the basic system catalog scanning functionality by providing ordered access to catalog tuples through an underlying index scan. Unlike the standard systable_getnext(), this function accepts a ScanDirection parameter that allows forward or backward navigation through the scan results in index order.

The function operates by calling index_getnext_slot() to retrieve the next tuple slot according to the specified direction, then converts the slot contents to a HeapTuple using ExecFetchSlotHeapTuple(). It includes error handling for lossy index conditions (which are not supported for system catalog scans) and handles concurrent transaction aborts that may occur during logical replication streaming.

## Parameters / Member Variables
- `sysscan`: SysScanDesc descriptor containing the scan state, including the index relation (irel), index scan (iscan), and tuple slot
- `direction`: ScanDirection enum value (ForwardScanDirection or BackwardScanDirection) specifying the iteration direction

## Dependencies
- Functions called/Symbols referenced:
  - [index_getnext_slot](../i/index_getnext_slot.md)
  - [ExecFetchSlotHeapTuple](../E/ExecFetchSlotHeapTuple.md)  
  - [HandleConcurrentAbort](../H/HandleConcurrentAbort.md)
  - [SysScanDesc](../S/SysScanDesc.md) (type)
  - ScanDirection (type)
- Called from (representative examples):
  - [toast_delete_datum](../t/toast_delete_datum.md)
  - [heap_fetch_toast_slice](../h/heap_fetch_toast_slice.md)
  - [inv_getsize](../i/inv_getsize.md)
  - [inv_read](../i/inv_read.md)
  - [inv_write](../i/inv_write.md)
  - [enum_endpoint](../e/enum_endpoint.md)
  - [BuildEventTriggerCache](../B/BuildEventTriggerCache.md)

## Notes and Other Information
- Requires an active index scan (sysscan->irel must be non-NULL)
- Does not support lossy index conditions - will throw an ERROR if encountered
- Includes special handling for concurrent transaction aborts during logical streaming
- The returned HeapTuple should not be freed by the caller as it references slot memory
- Part of the ordered scanning API that complements the basic systable_getnext() function