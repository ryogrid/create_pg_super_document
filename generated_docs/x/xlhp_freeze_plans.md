# xlhp_freeze_plans

## Location
[src/include/access/heapam_xlog.h:364-368](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/heapam_xlog.h#L364-L368)

## Overview
A container structure that holds an array of freeze plans for a heap page during VACUUM freeze operations, optimizing WAL space by grouping tuples with similar freezing characteristics.

## Definition
```c
typedef struct xlhp_freeze_plans
{
    uint16            nplans;
    xlhp_freeze_plan  plans[FLEXIBLE_ARRAY_MEMBER];
} xlhp_freeze_plans;
```

## Detailed Description
The xlhp_freeze_plans structure serves as a container for multiple freeze plans within a single heap page prune operation. This structure is a sub-record within xl_heap_prune WAL records and represents all the freezing operations that need to be performed on a page during VACUUM.

The structure contains an array of xlhp_freeze_plan elements, each describing how to freeze a group of tuples with identical characteristics. This design optimizes both WAL space usage and replay performance by grouping tuples that require the same freezing treatment. The individual tuple offsets for all plans are stored separately at the end of the entire prune record, organized in the same order as the plans.

As of PostgreSQL 17, this structure is part of the unified XLOG_HEAP2_PRUNE_VACUUM_SCAN records that replaced the separate XLOG_HEAP2_FREEZE_PAGE records, providing more efficient WAL logging for complex page operations.

## Parameters / Member Variables
- `nplans`: Number of freeze plans contained in the plans array
- `plans`: Variable-length array of xlhp_freeze_plan structures, each describing freeze operations for tuples with identical characteristics

## Dependencies
- Functions called/Symbols referenced:
  - [xlhp_freeze_plan](xlhp_freeze_plan.md) (individual freeze plan structure)
  - FLEXIBLE_ARRAY_MEMBER (C99 flexible array member)
- Called from (representative examples):
  - [log_heap_prune_and_freeze](../l/log_heap_prune_and_freeze.md) (src/backend/access/heap/pruneheap.c:2068, 2097)
  - [heap_xlog_deserialize_prune_and_freeze](../h/heap_xlog_deserialize_prune_and_freeze.md) (src/backend/access/rmgrdesc/heapdesc.c:113, 119)

## Notes and Other Information
- This is a variable-length structure due to the flexible array member
- Part of the xl_heap_prune WAL record sub-record system controlled by XLHP_HAS_FREEZE_PLANS flag
- Tuple offsets for all plans are stored separately in an array at the end of the entire record
- The offsets array contains nplans * (sum of each plan's ntuples) members in plan order
- Replaced separate XLOG_HEAP2_FREEZE_PAGE records as of PostgreSQL 17
- Enables efficient batch processing of freeze operations during VACUUM
- Critical for maintaining transaction visibility and preventing XID wraparound
- Used in both regular VACUUM operations and aggressive freezing scenarios