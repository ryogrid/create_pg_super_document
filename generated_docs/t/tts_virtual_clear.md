# tts_virtual_clear

## Location
[src/backend/executor/execTuples.c:108-129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L108-L129)

## Overview
Clears the contents of a virtual tuple table slot, freeing any allocated memory and resetting the slot to an empty state.

## Definition

```c
static void
tts_virtual_clear(TupleTableSlot *slot)
```
## Detailed Description
The  function is the clear callback for virtual tuple table slots in PostgreSQL. It is part of the  operations structure and is responsible for clearing the slot's contents while preserving the tuple descriptor.

The function performs the following operations:
1. If the slot has the  flag set (indicating it owns allocated memory), it frees the materialized data buffer and clears the flag
2. Resets the slot's validity count () to 0
3. Sets the  flag to indicate the slot is empty
4. Invalidates the tuple identifier ()

This function is typically called when a slot needs to be reused for a different tuple or when clearing slot contents during query execution.

## Parameters / Member Variables
- : A pointer to the TupleTableSlot to be cleared. This will be a VirtualTupleTableSlot structure that extends the base TupleTableSlot.

## Dependencies
- Functions called/Symbols referenced:
  - TTS_SHOULDFREE (macro to check if slot should free memory)
  - VirtualTupleTableSlot (cast to specific slot type)
  - [pfree](../p/pfree.md) (memory deallocation function)
  - TTS_FLAG_SHOULDFREE (flag indicating slot owns memory)
  - TTS_FLAG_EMPTY (flag indicating slot is empty)
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md) (function to invalidate tuple identifier)
- Called from (representative examples):
  - [tts_virtual_copyslot](tts_virtual_copyslot.md) (when copying slot contents)
  - Various slot clearing operations throughout the executor

## Notes and Other Information
- The function uses  hint for the memory freeing path, suggesting that most virtual slots don't have materialized data to free
- Virtual slots can be materialized (have a  buffer) when their contents need to persist beyond the slot's original memory context
- The clearing process preserves the slot's tuple descriptor, allowing the slot to be reused
- This is a key part of PostgreSQL's memory management strategy for tuple slots
- The function ensures proper cleanup of resources while maintaining the slot in a reusable state