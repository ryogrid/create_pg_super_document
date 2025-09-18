# TransitionTableAddTuple

## Location
src/backend/commands/trigger.c: 5584 - 5623

## Overview
Adds a tuple to a transition table tuplestore, applying necessary tuple conversion when dealing with inheritance hierarchies or partitioned tables.

## Definition
```c
static void TransitionTableAddTuple(EState *estate,
                                   TransitionCaptureState *transition_capture,
                                   ResultRelInfo *relinfo,
                                   TupleTableSlot *slot,
                                   TupleTableSlot *original_insert_tuple,
                                   Tuplestorestate *tuplestore)
```

## Detailed Description
TransitionTableAddTuple stores a tuple in the specified transition table tuplestore with proper format conversion when needed. The function handles three scenarios: direct storage of an original insert tuple, tuple conversion for inheritance/partitioning cases using attribute mapping, and direct storage when no conversion is required. It ensures that tuples stored in transition tables have the correct format expected by triggers, particularly important in inheritance hierarchies where child tables may have different column layouts than their parents.

The function operates through these paths:
1. **Early return**: If tuplestore is NULL, nothing needs to be done
2. **Original tuple path**: If original_insert_tuple is provided, store it directly without conversion
3. **Conversion path**: If a tuple conversion map exists (indicating inheritance/partitioning), convert the tuple using attribute mapping before storage
4. **Direct path**: Store the slot directly if no conversion is needed

## Parameters / Member Variables
- `estate`: Execution state containing context for the operation
- `transition_capture`: Structure containing transition table configuration and storage
- `relinfo`: Result relation information for determining conversion requirements
- `slot`: The tuple slot containing the data to be stored
- `original_insert_tuple`: Optional pre-converted tuple that can be stored directly
- `tuplestore`: The target tuplestore for the transition table

## Dependencies
- Functions called/Symbols referenced:
  - ExecGetChildToRootMap
  - GetAfterTriggersStoreSlot
  - execute_attr_map_slot
  - tuplestore_puttupleslot
- Types used:
  - EState
  - TransitionCaptureState
  - ResultRelInfo
  - TupleTableSlot
  - Tuplestorestate
  - TupleConversionMap
  - AfterTriggersTableData
- Called from (representative examples):
  - AfterTriggersTableData (src/backend/commands/trigger.c:3986)
  - AfterTriggerSaveEvent (src/backend/commands/trigger.c:6199, 6215)

## Notes and Other Information
- This is a static function, only accessible within the trigger.c file
- Handles tuple format conversion for inheritance and partitioning scenarios
- The original_insert_tuple optimization avoids redundant conversions when the correct format is already available
- Uses GetAfterTriggersStoreSlot to obtain a properly formatted slot for conversion operations
- Critical for ensuring trigger OLD/NEW tables contain tuples in the expected format regardless of the source table structure