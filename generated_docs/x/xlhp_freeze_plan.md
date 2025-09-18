# xlhp_freeze_plan

## Location
src/include/access/heapam_xlog.h: 341 - 350

## Overview
A structure representing a single freeze plan within a heap page prune operation, containing the transaction visibility and tuple header information needed to freeze a group of tuples with identical characteristics.

## Definition
```c
typedef struct xlhp_freeze_plan
{
    TransactionId xmax;
    uint16        t_infomask2;
    uint16        t_infomask;
    uint8         frzflags;

    /* Length of individual page offset numbers array for this plan */
    uint16        ntuples;
} xlhp_freeze_plan;
```

## Detailed Description
The xlhp_freeze_plan structure represents a single freeze plan that describes how to freeze a group of tuples that share identical freezing characteristics. During VACUUM freeze operations, tuples with the same transaction visibility properties and tuple header flags can be processed together using a single plan, which optimizes both WAL space usage and replay performance.

This structure is part of the xlhp_freeze_plans sub-record within xl_heap_prune WAL records. Multiple freeze plans can exist within a single prune operation, each handling tuples that require the same freezing treatment. The actual tuple offsets for each plan are stored separately in an array at the end of the entire record.

The freeze plan contains the target transaction ID and tuple header flags that will be applied to all tuples covered by this plan, along with specific freeze flags that control the freezing behavior.

## Parameters / Member Variables
- `xmax`: Transaction ID to be set as the xmax (deleting transaction) for tuples covered by this plan
- `t_infomask2`: Second tuple header bitmask to be applied to the frozen tuples
- `t_infomask`: Primary tuple header bitmask to be applied to the frozen tuples  
- `frzflags`: Freeze-specific flags controlling the freezing operation (XLH_FREEZE_XVAC, XLH_INVALID_XVAC)
- `ntuples`: Number of tuples that this freeze plan applies to

## Dependencies
- Functions called/Symbols referenced:
  - TransactionId (transaction identifier type)
- Called from (representative examples):
  - [heap_xlog_prune_freeze](../h/heap_xlog_prune_freeze.md) (src/backend/access/heap/heapam.c:9271)
  - [heap_log_freeze_eq](../h/heap_log_freeze_eq.md) (src/backend/access/heap/pruneheap.c:1896)
  - [heap_log_freeze_new_plan](../h/heap_log_freeze_new_plan.md) (src/backend/access/heap/pruneheap.c:1958)
  - [heap_log_freeze_plan](../h/heap_log_freeze_plan.md) (src/backend/access/heap/pruneheap.c:1979)
  - [log_heap_prune_and_freeze](../l/log_heap_prune_and_freeze.md) (src/backend/access/heap/pruneheap.c:2067, 2099)
  - [plan_elem_desc](../p/plan_elem_desc.md) (src/backend/access/rmgrdesc/heapdesc.c:77)
  - [xlhp_freeze_plans](xlhp_freeze_plans.md) (src/include/access/heapam_xlog.h:367)

## Notes and Other Information
- Used within xlhp_freeze_plans sub-records of xl_heap_prune WAL records
- Replaces the older separate XLOG_HEAP2_FREEZE_PAGE records (as of PostgreSQL 17)
- The frzflags field can contain combinations of freeze-specific constants like XLH_FREEZE_XVAC and XLH_INVALID_XVAC
- Tuple offsets for this plan are stored separately in an array at the end of the entire prune record
- Enables efficient grouping of tuples with identical freezing characteristics
- Critical for VACUUM freeze operations and maintaining transaction visibility during tuple freezing
- The ntuples field determines how many consecutive entries in the offset array belong to this plan