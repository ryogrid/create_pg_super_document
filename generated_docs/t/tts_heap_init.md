# tts_heap_init

## Location
[src/backend/executor/execTuples.c:316-320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L316-L320)

## Overview
tts_heap_init is an initialization callback function for HeapTupleTableSlot operations that performs no action, serving as a no-op implementation of the TupleTableSlotOps init callback.

## Definition

```c
static void
tts_heap_init(TupleTableSlot *slot)
```
## Detailed Description
This function is part of the TupleTableSlotOps interface for heap tuple table slots. It implements the init callback but has an empty function body, indicating that no special initialization is required for heap tuple slots beyond what is done during slot allocation. The function follows the PostgreSQL pattern where some callback implementations may be no-ops when the default initialization is sufficient.

## Parameters / Member Variables
- `slot`: Pointer to the TupleTableSlot being initialized (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - None (empty function body)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md) (indirectly through TupleTableSlotOps structure)

## Notes and Other Information
- This is a static function specific to heap tuple table slot operations
- Part of the TupleTableSlotOps callback interface defined in src/include/executor/tuptable.h
- The empty implementation suggests that heap tuple slots require no special initialization beyond default slot setup
- This function is referenced through the TupleTableSlotOps function pointer structure rather than called directly

## Simplified Source

```c
static void
tts_heap_init(TupleTableSlot *slot)
{
    // No special initialization required for heap tuple slots
    // Default slot setup is sufficient
}
```