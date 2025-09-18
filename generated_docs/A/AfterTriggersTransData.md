# AfterTriggersTransData

## Location
src/backend/commands/trigger.c: 3927 - 3935

## Overview
AfterTriggersTransData stores saved trigger state for subtransaction rollback support, preserving constraint states and event lists that can be restored when subtransactions abort.

## Definition
```c
struct AfterTriggersTransData
{
    /* these fields are just for resetting at subtrans abort: */
    SetConstraintState state;   /* saved S C state, or NULL if not yet saved */
    AfterTriggerEventList events;   /* saved list pointer */
    int         query_depth;    /* saved query_depth */
    CommandId   firing_counter; /* saved firing_counter */
};
```

## Detailed Description
AfterTriggersTransData implements the state preservation mechanism needed for PostgreSQL's subtransaction rollback functionality in the trigger system. When a subtransaction begins, the current trigger state is saved in this structure. If the subtransaction aborts, these saved values are restored to effectively "undo" any trigger-related changes made during the failed subtransaction. This ensures that trigger events, constraint states, and execution counters remain consistent across subtransaction boundaries.

## Parameters / Member Variables
- `state`: Saved SET CONSTRAINTS state from before the subtransaction started, NULL if no save was needed
- `events`: Snapshot of the trigger event list pointer from before the subtransaction
- `query_depth`: Saved query nesting depth to restore proper query stack state
- `firing_counter`: Saved firing counter value to maintain proper trigger execution ordering

## Dependencies
- Functions called/Symbols referenced:
  - SetConstraintState (for constraint state preservation)
  - AfterTriggerEventList (for event list preservation)
  - CommandId (for firing counter preservation)
- Called from (representative examples):
  - AfterTriggersData (trans_stack field)
  - AfterTriggerBeginSubXact

## Notes and Other Information
This structure is critical for maintaining ACID properties in PostgreSQL's trigger system when subtransactions are involved. The comment emphasizes that these fields are specifically for resetting during subtransaction abort scenarios. The structure is allocated as part of a dynamic array to support arbitrary nesting depths of subtransactions. The NULL-able state field optimizes memory usage by only saving constraint state when it has actually been modified during the subtransaction.