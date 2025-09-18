# AfterTriggerEventDataNoOids

## Location
[src/backend/commands/trigger.c:3746-3751](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L3746-L3751)

## Overview
AfterTriggerEventDataNoOids is a space-optimized variant of AfterTriggerEventData that excludes partition OID fields, used for trigger events that don't involve cross-partition operations.

## Definition
```c
/* AfterTriggerEventData, minus ate_src_part, ate_dst_part */
typedef struct AfterTriggerEventDataNoOids
{
    TriggerFlags ate_flags;
    ItemPointerData ate_ctid1;
    ItemPointerData ate_ctid2;
} AfterTriggerEventDataNoOids;
```

## Detailed Description
AfterTriggerEventDataNoOids is a memory-efficient variant of the full AfterTriggerEventData structure, specifically designed for trigger events that don't require partition OID information. This structure is used for regular table operations where cross-partition tuple movement is not involved.

By omitting the ate_src_part and ate_dst_part fields, this structure reduces memory consumption for the common case of trigger events on non-partitioned tables or partitioned table operations that don't cause row movement between partitions.

This structure is part of PostgreSQL's optimization strategy to minimize memory usage in the trigger event system by using the smallest structure that can contain the necessary information for each specific trigger event type.

## Parameters / Member Variables
- `ate_flags`: Status bits indicating trigger state (DONE/IN_PROGRESS) and type information, plus offset to shared trigger data
- `ate_ctid1`: Item pointer to the inserted, deleted, or old updated tuple
- `ate_ctid2`: Item pointer to the new updated tuple (used for UPDATE operations)

## Dependencies
- Functions called/Symbols referenced:
  - TriggerFlags (for ate_flags member)
- Called from (representative examples):
  - SizeofTriggerEvent (macro for size calculation)

## Notes and Other Information
- Used when AFTER_TRIGGER_2CTID flag is set but AFTER_TRIGGER_CP_UPDATE is not set
- Saves 8 bytes (two Oid fields) compared to the full AfterTriggerEventData structure
- Part of a family of optimized event structures that trade memory for storage efficiency
- Appropriate for UPDATE operations on regular tables or partitioned tables without cross-partition movement