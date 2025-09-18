# inv_close

## Location
src/backend/storage/large_object/inv_api.c: 337 - 348

## Overview
Closes a large object descriptor and releases its associated memory, complementing the inv_open function.

## Definition


## Detailed Description
The  function provides a clean way to close a large object descriptor that was previously created by . It performs validation to ensure the descriptor pointer is valid and then releases the long-term memory allocated for the descriptor structure. This function is essential for proper resource management in large object operations.

The function is straightforward in its implementation, serving as the counterpart to  in the large object lifecycle. It ensures that memory allocated in the memory context specified during  is properly freed.

## Parameters / Member Variables
- : Pointer to the LargeObjectDesc structure to be closed and freed

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid (for assertion checking)
  - pfree (for memory deallocation)
- Called from (representative examples):
  - lo_import_internal
  - be_lo_export
  - closeLOfd
  - lo_get_fragment_internal
  - be_lo_from_bytea
  - be_lo_put

## Notes and Other Information
- Must be called with a valid LargeObjectDesc pointer obtained from inv_open
- The function uses an assertion to validate the pointer, which will cause a failure in debug builds if passed a NULL or invalid pointer
- After calling this function, the obj_desc pointer becomes invalid and should not be used
- This is the proper way to clean up large object descriptors and prevent memory leaks
- The function does not affect the actual large object data in the database, only the local descriptor