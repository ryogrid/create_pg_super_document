# tts_virtual_getsysattr

## Location
[src/backend/executor/execTuples.c:141-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L141-L156)

## Overview
Attempts to retrieve system attributes from virtual tuple table slots, but throws an error as virtual slots do not support most system attributes.

## Definition


## Detailed Description
The  function is the getsysattr callback for virtual tuple table slots in PostgreSQL. This function is called when code attempts to access system attributes (like ctid, xmin, xmax, etc.) from a virtual tuple table slot.

Virtual tuple table slots are designed to store computed or derived tuple data and do not have access to the underlying heap tuple storage where system attributes reside. Therefore, most system attribute access is not supported and will result in an error.

The function first asserts that the slot is not empty, then reports a user-friendly error indicating that system columns cannot be retrieved in this context. Some system attributes like  may be handled generically by higher-level code before this function is called.

## Parameters / Member Variables
- : A pointer to the TupleTableSlot from which to retrieve the system attribute
- : The attribute number of the system column to retrieve (negative values for system attributes)
- : A pointer to a boolean that would be set to indicate if the attribute value is NULL (not used due to error)

## Dependencies
- Functions called/Symbols referenced:
  - TTS_EMPTY (macro to check if slot is empty)
  - Assert (debugging assertion)
  - ereport (error reporting function)
  - [errcode](../e/errcode.md) (error code specification)
  - [errmsg](../e/errmsg.md) (error message specification)
  - ERRCODE_FEATURE_NOT_SUPPORTED (specific error code)
- Called from (representative examples):
  - System attribute access operations throughout the executor
  - Referenced in TTSOpsVirtual.getsysattr callback

## Notes and Other Information
- Virtual slots represent computed tuples that don't have physical storage backing
- System attributes like ctid, xmin, xmax are tied to heap tuple storage and don't make sense for virtual tuples
- Some system attributes like tableoid may be handled at a higher level before reaching this function
- The error provides a clear message to users/developers about the limitation
- This design reflects the fundamental difference between virtual slots and storage-based slots
- The function returns 0 to silence compiler warnings, but this value is never used due to the error
- Virtual slots are commonly used for result tuples, intermediate computations, and projections where system attributes are not relevant