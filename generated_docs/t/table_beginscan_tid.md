# table_beginscan_tid

## Location
src/include/access/tableam.h: 996 - 1008

## Overview
table_beginscan_tid is a specialized table scanning function designed for TID (Tuple Identifier) scans, which directly access specific tuples by their physical location identifiers.

## Definition
```c
static inline TableScanDesc
table_beginscan_tid(Relation rel, Snapshot snapshot)
```

## Detailed Description
table_beginscan_tid is specifically designed for TID scan operations, which represent the most direct form of tuple access in PostgreSQL. TID scans access tuples directly by their physical location (block number and offset within block) rather than scanning through the table sequentially or using indexes. This function sets up a scan descriptor with the SO_TYPE_TIDSCAN flag and minimal configuration since TID scans don't require the complex options of other scan types.

TID scans are typically used when the exact physical location of desired tuples is known, such as when following ctid references or in certain system operations that need to access specific tuple versions.

## Parameters / Member Variables
- `rel`: The relation (table) to be scanned using TID scan
- `snapshot`: Snapshot for visibility checking of tuples during the scan

## Dependencies
- Functions called/Symbols referenced:
  - SO_TYPE_TIDSCAN (TID scan type flag)
  - rd_tableam->scan_begin (table access method function)
- Called from (representative examples):
  - TidListEval
  - currtid_internal

## Notes and Other Information
- Designed for direct tuple access using Tuple Identifiers (TIDs)
- Simplest of all table scan functions with minimal configuration options
- No scan keys are used since TID scans target specific physical locations
- TID scans bypass normal sequential or index-based access patterns
- Commonly used for following ctid chains in tuple updates or for system-level operations
- Does not require strategy, synchronization, or page-mode options since access is direct
- The absence of scan keys (nkeys=0, key=NULL) reflects the direct-access nature of TID scans