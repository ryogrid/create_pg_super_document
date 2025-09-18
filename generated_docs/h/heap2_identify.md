heap2_identify

## Overview
This function converts heap2 WAL record operation codes into human-readable string identifiers for debugging and logging purposes.

## Definition
const char *heap2_identify(uint8 info)

## Detailed Description
heap2_identify is a utility function that maps heap2-related WAL record operation codes to descriptive string names. Similar to heap_identify but specifically for heap2 operations, it processes the info parameter by masking off non-operation bits using XLR_INFO_MASK and uses a switch statement to identify the specific heap2 operation type.

The function handles all heap2 operations including:
- PRUNE operations (ON_ACCESS, VACUUM_SCAN, VACUUM_CLEANUP) for tuple pruning during different scenarios
- VISIBLE operations for visibility map updates
- MULTI_INSERT operations for bulk tuple insertions (with optional page initialization)
- LOCK_UPDATED operations for updating tuple locks
- NEW_CID operations for command ID management
- REWRITE operations for table rewrites

This function is essential for PostgreSQL debugging tools, log analysis, and WAL record inspection utilities when dealing with advanced heap operations.

## Parameters / Member Variables
- info: 8-bit unsigned integer containing the WAL record operation code and flags

## Dependencies
- Functions called/Symbols referenced:
  - XLR_INFO_MASK (constant for masking info bits)
  - XLOG_HEAP2_PRUNE_ON_ACCESS
  - XLOG_HEAP2_PRUNE_VACUUM_SCAN
  - XLOG_HEAP2_PRUNE_VACUUM_CLEANUP
  - XLOG_HEAP2_VISIBLE
  - XLOG_HEAP2_MULTI_INSERT
  - XLOG_HEAP_INIT_PAGE
  - XLOG_HEAP2_LOCK_UPDATED
  - XLOG_HEAP2_NEW_CID
  - XLOG_HEAP2_REWRITE
- Called from (representative examples):
  - WAL record identification infrastructure (indirectly through resource manager tables)

## Notes and Other Information
- Returns NULL for unrecognized operation codes
- The function distinguishes between different types of pruning operations based on their context (access, vacuum scan, vacuum cleanup)
- MULTI_INSERT operations can be combined with XLOG_HEAP_INIT_PAGE flag for page initialization
- This function complements heap_identify by handling more advanced heap operations
- Part of the heap2 resource manager description system
- Located in src/backend/access/rmgrdesc/heapdesc.c:430-466