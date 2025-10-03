# tts_virtual_getsomeattrs

## Location
[src/backend/executor/execTuples.c:130-140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L130-L140)

## Overview
A placeholder function that should never be called for virtual tuple table slots, as they always maintain fully populated attribute arrays.

## Definition

```c
static void
tts_virtual_getsomeattrs(TupleTableSlot *slot, int natts)
```
## Detailed Description
The  function is the getsomeattrs callback for virtual tuple table slots in PostgreSQL. However, unlike other slot implementations, this function is designed to never actually be called during normal operation.

Virtual tuple table slots differ from other slot types (like heap tuple slots) in that they always maintain fully populated  and  arrays. Other slot types might lazily populate these arrays as attributes are accessed, requiring the  function to extract attribute values from the underlying storage format (heap tuples, minimal tuples, etc.).

Since virtual slots store their data directly as Datum arrays, all attributes are immediately available without any deformation process. Therefore, calling this function indicates a logic error in the code, and the function responds by throwing an error.

## Parameters / Member Variables
- `*slot`: A pointer to the TupleTableSlot (unused in the implementation)
- `natts`: The number of attributes to extract (unused in the implementation)
## Dependencies
- Functions called/Symbols referenced:
  - elog (error logging function)
- Called from (representative examples):
  - Should never be called in normal operation
  - Referenced in TTSOpsVirtual.getsomeattrs callback

## Notes and Other Information
- This function serves as a safety check to catch programming errors
- Virtual slots are designed to have all their attribute values immediately available
- The error message helps developers identify when the slot abstraction is being used incorrectly
- This design reflects the fundamental difference between virtual slots and storage-based slots
- Virtual slots trade memory usage (storing all attributes) for access speed (no deformation required)
- The function is part of the tuple table slot interface but is intentionally unimplemented for virtual slots