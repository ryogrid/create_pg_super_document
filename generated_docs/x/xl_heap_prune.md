# xl_heap_prune

## Location
[src/include/access/heapam_xlog.h:284-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/heapam_xlog.h#L284-L293)

## Overview
A WAL record structure that captures heap page pruning and freezing operations performed by VACUUM or on-access pruning, supporting complex variable-length sub-records for different pruning activities.

## Definition
```c
typedef struct xl_heap_prune
{
    uint8       reason;
    uint8       flags;

    /*
     * If XLHP_HAS_CONFLICT_HORIZON is set, the conflict horizon XID follows,
     * unaligned
     */
} xl_heap_prune;
```

## Detailed Description
The xl_heap_prune structure serves as the main header for complex WAL records that encode VACUUM pruning, freezing, and on-access pruning page modifications. This structure is designed to handle variable-length records with multiple optional sub-records that capture different aspects of pruning operations.

The record format is highly flexible and space-efficient, with flags indicating which sub-records are included. Sub-records can include freeze plans (xlhp_freeze_plans), redirection information, dead item lists, and unused item lists. The structure supports both VACUUM-initiated pruning and opportunistic pruning that occurs during regular page access.

The design carefully considers memory alignment requirements, with the snapshot conflict horizon stored unaligned to save space, while ensuring that structures with TransactionId fields maintain proper 4-byte alignment.

## Parameters / Member Variables
- `reason`: Indicates the reason for the pruning operation (e.g., VACUUM, on-access pruning)
- `flags`: Bitmask indicating which sub-records follow and additional conditions for replay

## Dependencies
- Functions called/Symbols referenced:
  - (This is a data structure with conditional sub-records)
- Called from (representative examples):
  - [heap_xlog_prune_freeze](../h/heap_xlog_prune_freeze.md) (src/backend/access/heap/heapam.c:9215)
  - [log_heap_prune_and_freeze](../l/log_heap_prune_and_freeze.md) (src/backend/access/heap/pruneheap.c:2062)
  - [heap2_desc](../h/heap2_desc.md) (src/backend/access/rmgrdesc/heapdesc.c:270)
  - SizeOfHeapPrune (src/include/access/heapam_xlog.h:295)

## Notes and Other Information
- The flags field can contain combinations of XLHP_* constants:
  - XLHP_IS_CATALOG_REL: Indicates operation on a catalog relation for logical decoding
  - XLHP_CLEANUP_LOCK: Indicates if replay requires a cleanup lock vs. ordinary exclusive lock
  - XLHP_HAS_CONFLICT_HORIZON: Indicates that a snapshot conflict horizon XID follows
  - XLHP_HAS_FREEZE_PLANS: Indicates presence of xlhp_freeze_plans sub-record
  - XLHP_HAS_REDIRECTIONS: Indicates presence of redirection information
  - XLHP_HAS_DEAD_ITEMS: Indicates presence of dead item list
  - XLHP_HAS_NOW_UNUSED_ITEMS: Indicates presence of unused item list
- Sub-records appear in a specific order based on the XLHP_* flags set
- Supports complex multi-part records that can include freeze plans, pruning items, and offset arrays
- Critical for maintaining Hot Standby consistency through snapshot conflict resolution
- The SizeOfHeapPrune macro calculates only the fixed header size, excluding variable sub-records