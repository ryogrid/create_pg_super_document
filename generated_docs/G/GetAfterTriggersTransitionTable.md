# GetAfterTriggersTransitionTable

## Location
src/backend/commands/trigger.c: 5533 - 5583

## Overview
Retrieves the appropriate transition table (tuplestore) for storing old or new tuples based on the trigger event type and transition capture requirements.

## Definition
```c
static Tuplestorestate *GetAfterTriggersTransitionTable(int event,
                                                       TupleTableSlot *oldslot,
                                                       TupleTableSlot *newslot,
                                                       TransitionCaptureState *transition_capture)
```

## Detailed Description
GetAfterTriggersTransitionTable determines which transition table (tuplestore) should be used to capture tuples for OLD and NEW transition tables in triggers. The function examines the trigger event type (INSERT, UPDATE, DELETE), checks which transition tables are configured for capture, and returns the appropriate tuplestore. It handles special cases for UPDATE operations during partition-key row movement where OLD might be NULL for inserted rows and NEW might be NULL for deleted rows.

The function operates by:
1. Extracting transition capture configuration flags from the transition_capture parameter
2. Validating that appropriate slots are non-NULL for the given event type
3. Determining the correct tuplestore based on event type and slot availability:
   - For oldslot: returns old_del_tuplestore (DELETE) or old_upd_tuplestore (UPDATE)
   - For newslot: returns new_ins_tuplestore (INSERT) or new_upd_tuplestore (UPDATE)

## Parameters / Member Variables
- `event`: The trigger event type (TRIGGER_EVENT_INSERT, TRIGGER_EVENT_UPDATE, or TRIGGER_EVENT_DELETE)
- `oldslot`: TupleTableSlot containing the old tuple data (for DELETE and UPDATE operations)
- `newslot`: TupleTableSlot containing the new tuple data (for INSERT and UPDATE operations)
- `transition_capture`: Structure containing transition table configuration and the actual tuplestores

## Dependencies
- Functions called/Symbols referenced:
  - TupIsNull (macro)
- Types used:
  - Tuplestorestate
  - TupleTableSlot
  - TransitionCaptureState
- Constants:
  - TRIGGER_EVENT_DELETE
  - TRIGGER_EVENT_INSERT
  - TRIGGER_EVENT_UPDATE
- Called from (representative examples):
  - [AfterTriggersTableData](../A/AfterTriggersTableData.md) (src/backend/commands/trigger.c:3982)
  - AfterTriggerSaveEvent (src/backend/commands/trigger.c:6195, 6211)

## Notes and Other Information
- This is a static function, only accessible within the trigger.c file
- Handles special UPDATE cases during partition-key row movement where either OLD or NEW can be NULL
- Uses assertions to validate proper slot usage for each event type
- The function assumes mutual exclusivity between oldslot and newslot (exactly one should be non-NULL)
- Returns NULL when no appropriate tuplestore is found or configured for the given combination of event and slot