# systable_getnext

## Location
src/backend/access/index/genam.c: 505 - 563

## Overview
systable_getnext is a core function that retrieves the next tuple in a PostgreSQL system catalog scan, supporting both heap-based and index-based scanning strategies.

## Definition
```c
HeapTuple systable_getnext(SysScanDesc sysscan)
```

## Detailed Description
This function implements the tuple retrieval mechanism for system catalog scans. It handles two distinct scanning modes: index-based scans (when an index relation is available) and sequential heap scans (when no suitable index exists). The function fetches tuples using slot-based interfaces internally but returns traditional HeapTuple pointers for compatibility.

For index scans, it uses index_getnext_slot() to fetch the next tuple that matches the scan conditions, then converts the slot contents to a HeapTuple. For heap scans, it uses table_scan_getnextslot() to sequentially read through the table. In both cases, the function ensures that the returned tuple reference is valid and handles concurrent transaction abort scenarios by calling HandleConcurrentAbort().

The function includes safety checks for lossy index conditions, which are not currently supported for system catalog scans but could be implemented if needed.

## Parameters / Member Variables
- : A SysScanDesc structure containing the scan state, including pointers to the index relation (if any), scan descriptors, and tuple slots

## Dependencies
- Functions called/Symbols referenced:
  - index_getnext_slot (for index-based scans)
  - ForwardScanDirection (scan direction constant)
  - ExecFetchSlotHeapTuple (slot to HeapTuple conversion)
  - table_scan_getnextslot (for heap scans)
  - HandleConcurrentAbort (concurrent abort handling)
- Called from (representative examples):
  - toastrel_valueid_exists
  - systable_inplace_update_begin
  - RemoveRoleFromObjectACL
  - ExecGrant_Largeobject
  - GetNewOidWithIndex
  - findDependentObjects
  - SearchCatCacheMiss
  - RelationBuildTupleDesc

## Notes and Other Information
- Returns NULL when no more tuples are available in the scan
- The returned HeapTuple is a reference to data in a disk buffer and must not be modified
- The tuple should be presumed inaccessible after the next getnext() or endscan() call
- Includes error handling for lossy index operators, which are explicitly not supported for system catalog scans
- Comments suggest that a slot-based interface might be beneficial for future optimization
- This function is critical for PostgreSQL's catalog access infrastructure and is used extensively throughout the system for metadata operations