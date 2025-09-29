# table_beginscan

## Location
[src/include/access/tableam.h:909-932](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L909-L932)

## Overview
table_beginscan is a core table scanning function that initiates a sequential scan of a table relation with optional filtering capabilities through scan keys and snapshot visibility checks.

## Definition

```c
struct ScanKeyData *key)
{
	uint32		flags = SO_TYPE_SEQSCAN |
		SO_ALLOW_STRAT | SO_ALLOW_SYNC | SO_ALLOW_PAGEMODE;

	return rel->rd_tableam->scan_begin(rel, snapshot, nkeys, key, NULL, flags);
}

/*
 * Like table_beginscan(), but for scanning catalog. It'll automatically use a
 * snapshot appropriate for scanning catalog relations.
 */
extern TableScanDesc table_beginscan_catalog(Relation relation, int nkeys,
											 struct ScanKeyData *key);
```
## Detailed Description
table_beginscan is a high-level interface function that starts a table scan operation. It acts as a wrapper around the table access method's scan_begin function, providing a standardized way to initiate sequential scans across different table access methods. The function sets up default scan flags optimized for sequential scanning, including strategy allowance, synchronization, and page mode operations.

The function delegates the actual scan initialization to the table's access method handler through the rd_tableam->scan_begin function pointer, making it compatible with different storage engines (heap, columnar, etc.).

## Parameters
- `rel`: The relation (table) to be scanned
- `snapshot`: Snapshot for visibility checking of tuples during the scan
- `nkeys`: Number of scan keys for filtering (0 means no filtering)
- `key`: Array of ScanKeyData structures defining the filter conditions

## Dependencies
- Functions called/Symbols referenced:
  - SO_TYPE_SEQSCAN (scan type flag)
  - SO_ALLOW_STRAT (allows strategy optimization)
  - SO_ALLOW_SYNC (allows synchronized scanning)
  - SO_ALLOW_PAGEMODE (allows page-at-a-time reading)
  - rd_tableam->scan_begin (table access method function)
- Called from (representative examples):
  - [heapam_relation_copy_for_cluster](../h/heapam_relation_copy_for_cluster.md)
  - [DoCopyTo](../D/DoCopyTo.md)
  - [ATRewriteTable](../A/ATRewriteTable.md)
  - [SeqNext](../S/SeqNext.md)
  - [RelationFindReplTupleSeq](../R/RelationFindReplTupleSeq.md)

## Notes and Other Information
- This is an inline function defined in the tableam.h header for performance
- The function combines multiple scan option flags to optimize sequential scanning
- It provides a uniform interface that works across different table access methods
- The returned TableScanDesc should be used with other table scan functions and eventually closed with table_endscan

## Simplified Source

```c
static inline TableScanDesc table_beginscan(Relation rel, Snapshot snapshot,
                                           int nkeys, struct ScanKeyData *key) {
    // Set standard flags for sequential scanning
    uint32 flags = SO_TYPE_SEQSCAN |        // Sequential scan type
                   SO_ALLOW_STRAT |          // Allow strategy optimization
                   SO_ALLOW_SYNC |           // Allow synchronized scanning
                   SO_ALLOW_PAGEMODE;        // Allow page-at-a-time reading

    // Delegate to table access method's scan_begin function
    return rel->rd_tableam->scan_begin(rel, snapshot, nkeys, key, NULL, flags);
}
```