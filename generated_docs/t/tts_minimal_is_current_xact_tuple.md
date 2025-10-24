# tts_minimal_is_current_xact_tuple

## Location
[src/backend/executor/execTuples.c:574-585](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L574-L585)

## Overview
A slot operation function for minimal tuple table slots that throws an error when attempting to determine transaction information, as minimal tuples do not contain transaction-related metadata.

## Definition
```c
static bool tts_minimal_is_current_xact_tuple(TupleTableSlot *slot)
```

## Detailed Description
This function is part of the TupleTableSlot operations interface for minimal tuple slots. It handles requests to determine whether a tuple was created or modified in the current transaction. Since minimal tuples are a compact representation that excludes transaction information (such as xmin, xmax, cmin, cmax), this operation is not supported for minimal tuple slots.

The function is designed to fail gracefully when transaction information is requested from a minimal tuple, providing a clear error message rather than returning incorrect or undefined results. This reflects the fundamental design trade-off of minimal tuples: they sacrifice transaction visibility information to achieve a more compact representation suitable for temporary results and inter-process communication.

## Parameters / Member Variables
- `slot`: Pointer to the TupleTableSlot containing the minimal tuple

## Dependencies
- Functions called/Symbols referenced:
  - TTS_EMPTY (macro for checking if slot is empty)
  - ereport (error reporting function)
  - [errcode](../e/errcode.md) (error code specification)
  - [errmsg](../e/errmsg.md) (error message formatting)
- Called from (representative examples):
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- This is a static function, only accessible within execTuples.c
- The function always throws an ERROR and never returns normally
- The return statement (return false) is only present to silence compiler warnings about missing return values
- Transaction information queries are typically used for visibility checks and MVCC (Multi-Version Concurrency Control) operations
- Minimal tuples are often used in contexts where transaction information is not needed, such as temporary results during query execution
- The error code ERRCODE_FEATURE_NOT_SUPPORTED clearly indicates this is an intentional design limitation
- This function would typically be called indirectly through the slot operations vtable when code attempts transaction-related operations on minimal tuple slots

## Simplified Source

```c
static bool tts_minimal_is_current_xact_tuple(TupleTableSlot *slot)
{
    // Verify slot is not empty
    Assert(!TTS_EMPTY(slot));

    // Minimal tuples don't contain transaction info - report error
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("don't have transaction information for this type of tuple")));

    return false; // Never reached - silences compiler warnings
}
```