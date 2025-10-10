# heap_xlog_deserialize_prune_and_freeze

## Location
[src/backend/access/rmgrdesc/heapdesc.c:104-182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/heapdesc.c#L104-L182)

## Overview
A critical deserialization function that unpacks complex heap prune and freeze WAL record data into separate component arrays, enabling both WAL replay and description functionality.

## Definition
```c
void heap_xlog_deserialize_prune_and_freeze(char *cursor, uint8 flags,
                                           int *nplans, xlhp_freeze_plan **plans,
                                           OffsetNumber **frz_offsets,
                                           int *nredirected, OffsetNumber **redirected,
                                           int *ndead, OffsetNumber **nowdead,
                                           int *nunused, OffsetNumber **nowunused)
```

## Detailed Description
The `heap_xlog_deserialize_prune_and_freeze` function is a sophisticated deserialization utility that parses the binary data from XLOG_HEAP2_PRUNE_* WAL records into their constituent components. It takes a MAXALIGNed buffer containing packed WAL record data and systematically extracts various arrays of information based on the provided flags.

The function handles four main categories of data: freeze plans (for tuple freezing operations), item redirections (for HOT chain management), dead items (tuples to be marked as dead), and now-unused items (line pointers to be cleared). Each category is conditionally processed based on flags, allowing the WAL record format to be compact by only including relevant data.

This function is essential for both WAL replay during recovery (via heap_xlog_prune_freeze) and WAL description for debugging tools like pg_waldump (via heap2_desc). The shared implementation ensures consistency between replay and description operations.

## Parameters / Member Variables
- `cursor`: Pointer to MAXALIGNed buffer containing the serialized WAL record data
- `flags`: Bitmask indicating which data categories are present in the record
- `nplans`: Output parameter for number of freeze plans
- `plans`: Output parameter for array of freeze plan structures
- `frz_offsets`: Output parameter for freeze offsets array
- `nredirected`: Output parameter for number of redirected items
- `redirected`: Output parameter for array of redirection offset pairs
- `ndead`: Output parameter for number of dead items
- `nowdead`: Output parameter for array of dead item offsets
- `nunused`: Output parameter for number of now-unused items
- `nowunused`: Output parameter for array of now-unused item offsets

## Dependencies
- Functions called/Symbols referenced:
  - offsetof (C standard macro)
  - [xlhp_freeze_plans](../x/xlhp_freeze_plans.md) (struct type)
  - [xlhp_freeze_plan](../x/xlhp_freeze_plan.md) (struct type)
  - [xlhp_prune_items](../x/xlhp_prune_items.md) (struct type)
  - OffsetNumber (type)
  - XLHP_HAS_FREEZE_PLANS (flag constant)
  - XLHP_HAS_REDIRECTIONS (flag constant)
  - XLHP_HAS_DEAD_ITEMS (flag constant)
  - XLHP_HAS_NOW_UNUSED_ITEMS (flag constant)
- Called from:
  - [heap_xlog_prune_freeze](heap_xlog_prune_freeze.md) (WAL replay)
  - [heap2_desc](heap2_desc.md) (WAL description)

## Notes and Other Information
- Located in heapdesc.c to enable sharing between backend recovery code and frontend pg_waldump utility
- Uses cursor advancement technique to parse variable-length record structures sequentially
- Handles optional data sections gracefully by setting counts to 0 and pointers to NULL when flags indicate absence
- Critical for PostgreSQLs tuple visibility and space management through pruning and freezing operations

## Simplified Source

```c
void
heap_xlog_deserialize_prune_and_freeze(char *cursor, uint8 flags,
                                       int *nplans, xlhp_freeze_plan **plans,
                                       OffsetNumber **frz_offsets,
                                       int *nredirected, OffsetNumber **redirected,
                                       int *ndead, OffsetNumber **nowdead,
                                       int *nunused, OffsetNumber **nowunused)
{
    // Parse freeze plans if present
    if (flags & XLHP_HAS_FREEZE_PLANS) {
        xlhp_freeze_plans *freeze_plans = (xlhp_freeze_plans *) cursor;
        *nplans = freeze_plans->nplans;
        *plans = freeze_plans->plans;
        cursor += offsetof(xlhp_freeze_plans, plans) + sizeof(xlhp_freeze_plan) * *nplans;
    } else {
        *nplans = 0;
        *plans = NULL;
    }

    // Parse redirections if present
    if (flags & XLHP_HAS_REDIRECTIONS) {
        xlhp_prune_items *subrecord = (xlhp_prune_items *) cursor;
        *nredirected = subrecord->ntargets;
        *redirected = &subrecord->data[0];
        cursor += offsetof(xlhp_prune_items, data) + sizeof(OffsetNumber[2]) * *nredirected;
    } else {
        *nredirected = 0;
        *redirected = NULL;
    }

    // Parse dead items if present
    if (flags & XLHP_HAS_DEAD_ITEMS) {
        xlhp_prune_items *subrecord = (xlhp_prune_items *) cursor;
        *ndead = subrecord->ntargets;
        *nowdead = subrecord->data;
        cursor += offsetof(xlhp_prune_items, data) + sizeof(OffsetNumber) * *ndead;
    } else {
        *ndead = 0;
        *nowdead = NULL;
    }

    // Parse now-unused items if present
    if (flags & XLHP_HAS_NOW_UNUSED_ITEMS) {
        xlhp_prune_items *subrecord = (xlhp_prune_items *) cursor;
        *nunused = subrecord->ntargets;
        *nowunused = subrecord->data;
        cursor += offsetof(xlhp_prune_items, data) + sizeof(OffsetNumber) * *nunused;
    } else {
        *nunused = 0;
        *nowunused = NULL;
    }

    // Remaining data is freeze offsets array
    *frz_offsets = (OffsetNumber *) cursor;
}
```