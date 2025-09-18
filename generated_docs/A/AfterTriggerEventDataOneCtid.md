# AfterTriggerEventDataOneCtid

## Location
src/backend/commands/trigger.c: 3754 - 3758

## Overview
AfterTriggerEventDataOneCtid is a space-optimized variant of AfterTriggerEventData that contains only one CTID field, used for INSERT and DELETE trigger events that don't require cross-partition information.

## Definition
```c
/* AfterTriggerEventData, minus ate_*_part and ate_ctid2 */
typedef struct AfterTriggerEventDataOneCtid
{
    TriggerFlags ate_flags;     /* status bits and offset to shared data */
    ItemPointerData ate_ctid1;  /* inserted, deleted, or old updated tuple */
} AfterTriggerEventDataOneCtid;
```

## Detailed Description
AfterTriggerEventDataOneCtid is a memory-efficient variant of AfterTriggerEventData designed for trigger events that require only a single tuple identifier. This structure is primarily used for INSERT and DELETE operations, which by their nature only reference one tuple (the newly inserted tuple or the deleted tuple).

By excluding the ate_ctid2 field (used for the new tuple in UPDATE operations) and the partition OID fields (ate_src_part and ate_dst_part), this structure significantly reduces memory consumption for single-tuple trigger events.

This optimization is particularly important for high-volume OLTP workloads where many INSERT and DELETE operations occur, as it reduces the memory footprint of the trigger event queue.

## Parameters / Member Variables
- `ate_flags`: Status bits indicating trigger state (DONE/IN_PROGRESS) and type information, plus offset to shared trigger data
- `ate_ctid1`: Item pointer to the inserted, deleted, or old updated tuple

## Dependencies
- Functions called/Symbols referenced:
  - TriggerFlags (for ate_flags member)
- Called from (representative examples):
  - SizeofTriggerEvent (macro for size calculation)

## Notes and Other Information
- Used when AFTER_TRIGGER_1CTID flag is set in ate_flags
- Saves 14 bytes compared to the full AfterTriggerEventData structure (one ItemPointerData + two Oid fields)
- Appropriate for INSERT, DELETE, and statement-level trigger events
- Statement-level triggers always use this format even though they don't actually need the ctid field
- Part of PostgreSQL's memory optimization strategy for the trigger event system