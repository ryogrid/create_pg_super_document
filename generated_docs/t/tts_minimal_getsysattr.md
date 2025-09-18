# tts_minimal_getsysattr

## Location
src/backend/executor/execTuples.c: 557 - 573

## Overview
A slot operation function for minimal tuple table slots that throws an error when attempting to retrieve system attributes, as minimal tuples do not support system columns.

## Definition
```c
static Datum tts_minimal_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
```

## Detailed Description
This function is part of the TupleTableSlot operations interface for minimal tuple slots. Unlike regular heap tuples that contain system attributes (such as ctid, xmin, xmax, etc.), minimal tuples are a compact representation that omits system columns to save space and improve performance. When code attempts to access system attributes from a minimal tuple slot, this function provides a clear error message indicating that system columns are not available in this context.

The function serves as a safeguard in the slot operations vtable, ensuring that any attempt to retrieve system attributes from minimal tuple slots fails gracefully with a descriptive error message rather than causing undefined behavior or silent failures.

## Parameters / Member Variables
- `slot`: Pointer to the TupleTableSlot (cast to MinimalTupleTableSlot internally)
- `attnum`: The attribute number of the system column being requested (negative values for system attributes)
- `isnull`: Pointer to boolean flag that would indicate if the attribute value is NULL (unused in this error case)

## Dependencies
- Functions called/Symbols referenced:
  - TTS_EMPTY (macro for checking if slot is empty)
  - ereport (error reporting function)
  - errcode (error code specification)
  - errmsg (error message formatting)
- Called from (representative examples):
  - slot_deform_heap_tuple

## Notes and Other Information
- This is a static function, only accessible within execTuples.c
- The function always throws an ERROR and never returns normally
- The return statement (return 0) is only present to silence compiler warnings about missing return values
- System attributes in PostgreSQL include ctid, tableoid, xmin, xmax, cmin, cmax, which are not stored in minimal tuples
- This design choice reflects the trade-off between space efficiency and functionality in minimal tuples
- The error code ERRCODE_FEATURE_NOT_SUPPORTED clearly indicates this is an intentional limitation rather than a bug