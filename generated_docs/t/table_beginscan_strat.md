# table_beginscan_strat

## Location
[src/include/access/tableam.h:933-953](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L933-L953)

## Overview
table_beginscan_strat is an extended version of table_beginscan that provides fine-grained control over buffer access strategy and synchronization options during table scanning operations.

## Definition
```c
static inline TableScanDesc
table_beginscan_strat(Relation rel, Snapshot snapshot,
                      int nkeys, struct ScanKeyData *key,
                      bool allow_strat, bool allow_sync)
```

## Detailed Description
table_beginscan_strat offers enhanced control over table scanning behavior compared to the basic table_beginscan function. It allows callers to explicitly specify whether nondefault buffer access strategies can be used and whether synchronized scanning can be enabled. This function is particularly useful in scenarios where specific performance characteristics are required, such as during index building or system catalog operations where synchronized scanning might interfere with expected behavior.

The function conditionally sets the SO_ALLOW_STRAT and SO_ALLOW_SYNC flags based on the boolean parameters, providing more precise control over the scan's resource usage patterns.

## Parameters / Member Variables
- `rel`: The relation (table) to be scanned
- `snapshot`: Snapshot for visibility checking of tuples during the scan
- `nkeys`: Number of scan keys for filtering (0 means no filtering)
- `key`: Array of ScanKeyData structures defining the filter conditions
- `allow_strat`: Whether to allow nondefault buffer access strategies
- `allow_sync`: Whether to allow synchronized scanning (may not start from block zero)

## Dependencies
- Functions called/Symbols referenced:
  - SO_TYPE_SEQSCAN (scan type flag)
  - SO_ALLOW_PAGEMODE (allows page-at-a-time reading)
  - SO_ALLOW_STRAT (conditionally set based on allow_strat parameter)
  - SO_ALLOW_SYNC (conditionally set based on allow_sync parameter)
  - rd_tableam->scan_begin (table access method function)
- Called from (representative examples):
  - [heapam_index_build_range_scan](../h/heapam_index_build_range_scan.md)
  - [heapam_index_validate_scan](../h/heapam_index_validate_scan.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [IndexCheckExclusion](../I/IndexCheckExclusion.md)

## Notes and Other Information
- This function provides more granular control compared to table_beginscan
- Commonly used in index building and validation operations where synchronized scanning behavior needs to be controlled
- The allow_strat parameter controls whether alternative buffer management strategies can be employed
- The allow_sync parameter is crucial for operations that require deterministic scanning order
- Both strategy and sync options default to true in the basic table_beginscan function

## Simplified Source

```c
// Simplified version of table_beginscan_strat
static inline TableScanDesc
table_beginscan_strat(Relation rel, Snapshot snapshot,
                      int nkeys, struct ScanKeyData *key,
                      bool allow_strat, bool allow_sync)
{
    // Set base flags for sequential scan with page mode
    uint32 flags = SO_TYPE_SEQSCAN | SO_ALLOW_PAGEMODE;

    // Conditionally enable buffer access strategy
    if (allow_strat)
        flags |= SO_ALLOW_STRAT;

    // Conditionally enable synchronized scanning
    if (allow_sync)
        flags |= SO_ALLOW_SYNC;

    // Delegate to table access method's scan_begin function
    return rel->rd_tableam->scan_begin(rel, snapshot, nkeys, key, NULL, flags);
}
```

Key simplifications made:
- Added clear comments explaining each logical step
- Preserved the essential flag-setting logic
- Maintained the conditional flag assignment based on parameters
- Kept the core delegation to the table access method
- Simplified variable declarations and formatting for readability