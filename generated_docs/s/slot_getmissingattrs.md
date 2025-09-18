# slot_getmissingattrs

## Location
[src/backend/executor/execTuples.c:1955-1988](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L1955-L1988)

## Overview
Fills in missing values for attributes in a TupleTableSlot, handling both cases where missing value arrays exist and where they don't.

## Definition
```c
void slot_getmissingattrs(TupleTableSlot *slot, int startAttNum, int lastAttNum)
```

## Detailed Description
This function fills in missing attribute values in a TupleTableSlot for a range of attributes from startAttNum to lastAttNum. It serves as a utility function primarily used during tuple deforming operations, particularly in JIT-compiled code paths. The function handles two scenarios:

1. When no missing values array exists in the tuple descriptor's constraints, it simply fills all specified attributes with NULL values
2. When a missing values array is present, it processes each attribute individually, setting the appropriate value from the missing values array or NULL if the attribute is marked as not present

The function is specifically exposed for JIT compiled tuple deforming and should generally not be called from outside execTuples.c.

## Parameters / Member Variables
- `slot`: The TupleTableSlot to fill with missing attribute values
- `startAttNum`: The starting attribute number (0-based index) to begin filling
- `lastAttNum`: The ending attribute number (exclusive) to stop filling

## Dependencies
- Functions called/Symbols referenced:
  - AttrMissing (struct type for missing value information)
  - memset (for bulk NULL/false assignment)
- Called from (representative examples):
  - [slot_getsomeattrs_int](slot_getsomeattrs_int.md)
  - JIT compiled tuple deforming code

## Notes and Other Information
- This function is primarily intended for internal use within the executor and JIT compilation
- The function assumes that the slot's tts_values and tts_isnull arrays are properly allocated
- When no missing values array exists, the function efficiently uses memset for bulk operations
- The missing values array is accessed through the tuple descriptor's constraint information
- Attribute numbering follows PostgreSQL's 0-based indexing for internal operations