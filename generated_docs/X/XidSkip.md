# XidSkip

## Location
[src/test/modules/xid_wraparound/xid_wraparound.c:171-199](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/xid_wraparound/xid_wraparound.c#L171-L199)

## Overview
An optimization function that calculates how many transaction IDs can be safely skipped to reach the next "interesting" boundary in PostgreSQL's SLRU (Simple LRU) structures.

## Definition
```c
static inline uint32 XidSkip(FullTransactionId fullxid)
```

## Detailed Description
This internal function implements an optimization for transaction ID consumption by determining how many XIDs can be skipped to efficiently reach the next significant boundary. The function considers boundaries in three SLRU structures: COMMIT_TS (commit timestamp), SUBTRANS (subtransaction), and CLOG (commit log). It calculates the minimum distance to the next page boundary across all three SLRUs, avoiding the overhead of individual XID processing when large jumps are possible. The function returns 0 if the current XID is already at an interesting boundary or too close to wraparound limits.

## Parameters / Member Variables
- `fullxid`: The current FullTransactionId to calculate skip distance from

## Dependencies
- Functions called/Symbols referenced:
  - XidFromFullTransactionId (extracts 32-bit XID from FullTransactionId)
  - COMMIT_TS_XACTS_PER_PAGE (constant defining transactions per commit timestamp page)
  - SUBTRANS_XACTS_PER_PAGE (constant defining transactions per subtransaction page)
  - CLOG_XACTS_PER_PAGE (constant defining transactions per commit log page)
  - Min (macro for minimum value calculation)
- Called from:
  - [consume_xids_shortcut](../c/consume_xids_shortcut.md) (uses this function to determine skip distances)

## Notes and Other Information
- This is a static inline function for performance optimization
- Located in src/test/modules/xid_wraparound/ as part of XID wraparound testing infrastructure
- Returns 0 for XIDs within 5 of boundaries (low < 5 or low >= UINT32_MAX - 5) to avoid wraparound issues
- Considers page boundaries for three different SLRU structures to ensure consistency
- Used as part of the shortcut mechanism to avoid expensive individual XID processing
- The function aims to skip to just before the next SLRU page extension point where interesting processing occurs