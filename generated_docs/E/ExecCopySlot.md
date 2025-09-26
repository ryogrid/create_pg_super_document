# ExecCopySlot

## Location
src/include/executor/tuptable.h: 509 - 523

## Overview
ExecCopySlot is a static inline function that efficiently copies the contents from one tuple table slot to another, utilizing the slot's specific copy implementation through its operation interface.

## Definition


## Detailed Description
ExecCopySlot provides a high-level interface for copying tuple data between TupleTableSlot instances. The function performs several important validations before delegating the actual copying operation to the destination slot's specific copyslot implementation through its tts_ops interface.

The function is designed to work with PostgreSQL's tuple table slot abstraction, which allows different storage formats (heap tuples, minimal tuples, virtual tuples, etc.) to be handled uniformly. The actual copying behavior depends on the specific slot type and its associated TupleTableSlotOps implementation.

Key characteristics:
- Performs runtime assertions to ensure data integrity
- Delegates to type-specific copy implementations
- Maintains slot abstraction boundaries
- Returns the destination slot for call chaining

## Parameters / Member Variables
- : The destination TupleTableSlot where the copied data will be stored. Must be a valid, initialized slot with the same tuple descriptor as the source
- : The source TupleTableSlot containing the data to be copied. Must not be empty and must have the same number of attributes as the destination

## Dependencies
- Functions called/Symbols referenced:
  - TTS_EMPTY (macro to check if slot is empty)
  - copyslot (callback function from TupleTableSlotOps)
- Called from (representative examples):
  - CopyFrom (COPY command processing)
  - ExecBRUpdateTriggersNew (trigger execution)
  - AfterTriggerExecute (trigger processing)
  - EvalPlanQual (EPQ processing)
  - ExecGroup (GROUP BY processing)
  - ExecIncrementalSort (incremental sorting)
  - ExecLimit (LIMIT clause processing)
  - ExecMaterial (materialization node)
  - ExecMemoize (memoization node)
  - ExecInsert (INSERT operation)
  - ExecUnique (UNIQUE processing)
  - ExecWindowAgg (window aggregation)

## Notes and Other Information
- **Assertion Requirements**: The function includes three critical assertions:
  1. Source slot must not be empty (TTS_EMPTY check)
  2. Source and destination slots must be different objects
  3. Both slots must have the same number of attributes (natts)

- **Future Enhancement**: The code comments indicate that future versions may relax the requirement for identical attribute counts, potentially allowing source slots with additional attributes where only leading attributes are copied.

- **Performance**: Being declared as , this function is typically inlined by the compiler, eliminating function call overhead in performance-critical paths.

- **Memory Context**: The copying operation respects the memory contexts associated with each slot, with the destination slot's context being used for any new allocations.

- **System Attributes**: When system attributes need to be accessed in the target slot, both source and destination slot types must match exactly to ensure proper handling of system columns.

- **Implementation Flexibility**: The actual copying logic is implemented by the slot's specific copyslot callback, allowing different slot types (HeapTupleTableSlot, VirtualTupleTableSlot, etc.) to optimize the copy operation according to their internal data structures.