# systable_getnext

## Location
[src/backend/access/index/genam.c:505-563](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/index/genam.c#L505-L563)

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
  - [index_getnext_slot](../i/index_getnext_slot.md) (for index-based scans)
  - ForwardScanDirection (scan direction constant)
  - [ExecFetchSlotHeapTuple](../E/ExecFetchSlotHeapTuple.md) (slot to HeapTuple conversion)
  - [table_scan_getnextslot](../t/table_scan_getnextslot.md) (for heap scans)
  - [HandleConcurrentAbort](../H/HandleConcurrentAbort.md) (concurrent abort handling)
- Called from (representative examples):
  - [toastrel_valueid_exists](../t/toastrel_valueid_exists.md)
  - [systable_inplace_update_begin](systable_inplace_update_begin.md)
  - [RemoveRoleFromObjectACL](../R/RemoveRoleFromObjectACL.md)
  - [ExecGrant_Largeobject](../E/ExecGrant_Largeobject.md)
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md)
  - [findDependentObjects](../f/findDependentObjects.md)
  - [SearchCatCacheMiss](../S/SearchCatCacheMiss.md)
  - [RelationBuildTupleDesc](../R/RelationBuildTupleDesc.md)

## Notes and Other Information
- Returns NULL when no more tuples are available in the scan
- The returned HeapTuple is a reference to data in a disk buffer and must not be modified
- The tuple should be presumed inaccessible after the next getnext() or endscan() call
- Includes error handling for lossy index operators, which are explicitly not supported for system catalog scans
- Comments suggest that a slot-based interface might be beneficial for future optimization
- This function is critical for PostgreSQL's catalog access infrastructure and is used extensively throughout the system for metadata operations

## Simplified Source

```c
// Simplified version of systable_getnext
HeapTuple systable_getnext(SysScanDesc sysscan) {
    HeapTuple htup = NULL;

    // Check if we're using an index scan
    if (sysscan->irel) {
        // Index-based scan: get next tuple from index
        if (index_getnext_slot(sysscan->iscan, ForwardScanDirection, sysscan->slot)) {
            // Convert slot to HeapTuple
            bool shouldFree;
            htup = ExecFetchSlotHeapTuple(sysscan->slot, false, &shouldFree);

            // Verify we don't need lossy index operations (not supported)
            if (sysscan->iscan->xs_recheck) {
                elog(ERROR, "system catalog scans with lossy index conditions are not implemented");
            }
        }
    } else {
        // Heap scan: sequential scan through table
        if (table_scan_getnextslot(sysscan->scan, ForwardScanDirection, sysscan->slot)) {
            // Convert slot to HeapTuple
            bool shouldFree;
            htup = ExecFetchSlotHeapTuple(sysscan->slot, false, &shouldFree);
        }
    }

    // Handle any concurrent transaction aborts
    HandleConcurrentAbort();

    return htup; // NULL if no more tuples
}
```

Key simplifications made:
- Removed verbose comments while keeping essential logic explanations
- Consolidated duplicate tuple extraction logic with inline comments
- Simplified variable declarations and assertions
- Maintained the core branching logic between index and heap scans
- Preserved critical error handling for lossy index conditions
- Kept essential concurrent abort handling