# heap_xlog_deserialize_prune_and_freeze

## Location
src/backend/access/rmgrdesc/heapdesc.c: 104 - 182

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
  - xlhp_freeze_plans (struct type)
  - xlhp_freeze_plan (struct type)
  - xlhp_prune_items (struct type)
  - OffsetNumber (type)
  - XLHP_HAS_FREEZE_PLANS (flag constant)
  - XLHP_HAS_REDIRECTIONS (flag constant)
  - XLHP_HAS_DEAD_ITEMS (flag constant)
  - XLHP_HAS_NOW_UNUSED_ITEMS (flag constant)
- Called from:
  - heap_xlog_prune_freeze (WAL replay)
  - heap2_desc (WAL description)

## Notes and Other Information
- Located in heapdesc.c to enable sharing between backend recovery code and frontend pg_waldump utility
- Uses cursor advancement technique to parse variable-length record structures sequentially
- Handles optional data sections gracefully by setting counts to 0 and pointers to NULL when flags indicate absence
- Critical for PostgreSQLs tuple visibility and space management through pruning and freezing operations