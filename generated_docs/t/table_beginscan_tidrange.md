# table_beginscan_tidrange

## Location
[src/include/access/tableam.h:1085-1105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1085-L1105)

## Overview
Entry point for setting up a TableScanDesc for a TID range scan, allowing scanning of tuples within a specified range of tuple identifiers.

## Definition

```c
static inline TableScanDesc
table_beginscan_tidrange(Relation rel, Snapshot snapshot,
						 ItemPointer mintid,
						 ItemPointer maxtid)
```
## Detailed Description
This function initializes a table scan descriptor specifically for TID (Tuple Identifier) range scanning. It creates a scan that will only examine tuples whose TIDs fall within the specified range from mintid to maxtid. The function sets up the scan with appropriate flags for TID range scanning and page mode operation, then configures the TID range using the table access method's scan_set_tidrange function.

The scan is configured with SO_TYPE_TIDRANGESCAN and SO_ALLOW_PAGEMODE flags to optimize for range-based tuple retrieval and enable efficient page-level operations.

## Parameters / Member Variables
- `rel`: The relation (table) to scan
- `snapshot`: The snapshot to use for visibility checking during the scan
- `mintid`: Pointer to the minimum TID (starting point of the range)
- `maxtid`: Pointer to the maximum TID (ending point of the range)
## Dependencies
- Functions called/Symbols referenced:
  - [TableScanDesc](../T/TableScanDesc.md) (return type)
  - SO_TYPE_TIDRANGESCAN (scan flag)
  - SO_ALLOW_PAGEMODE (scan flag)
  - rel->rd_tableam->scan_begin (table access method function)
  - rel->rd_tableam->scan_set_tidrange (table access method function)
- Called from (representative examples):
  - [TidRangeNext](../T/TidRangeNext.md)

## Notes and Other Information
- This is an inline function defined in the table access method header
- The function combines general scan initialization with TID range-specific configuration
- Used primarily by the TID range scan executor node for efficient tuple retrieval within specified TID boundaries
- The scan flags enable both type-specific optimization and page-mode operation for better performance

## Simplified Source

```c
static inline TableScanDesc
table_beginscan_tidrange(Relation rel, Snapshot snapshot,
                        ItemPointer mintid,
                        ItemPointer maxtid)
{
    TableScanDesc sscan;
    uint32 flags = SO_TYPE_TIDRANGESCAN | SO_ALLOW_PAGEMODE;

    // Initialize table scan with TID range scan flags
    sscan = rel->rd_tableam->scan_begin(rel, snapshot, 0, NULL, NULL, flags);

    // Configure the TID range to scan
    sscan->rs_rd->rd_tableam->scan_set_tidrange(sscan, mintid, maxtid);

    return sscan;
}
```