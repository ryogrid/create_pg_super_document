# inv_close

## Location
[src/backend/storage/large_object/inv_api.c:337-348](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/large_object/inv_api.c#L337-L348)

## Overview
Closes a large object descriptor and releases its associated memory, complementing the inv_open function.

## Definition

```c
void
inv_close(LargeObjectDesc *obj_desc)
```
## Detailed Description
The  function provides a clean way to close a large object descriptor that was previously created by . It performs validation to ensure the descriptor pointer is valid and then releases the long-term memory allocated for the descriptor structure. This function is essential for proper resource management in large object operations.

The function is straightforward in its implementation, serving as the counterpart to  in the large object lifecycle. It ensures that memory allocated in the memory context specified during  is properly freed.

## Parameters / Member Variables
- `*obj_desc`: Pointer to the LargeObjectDesc structure to be closed and freed
## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid (for assertion checking)
  - [pfree](../p/pfree.md) (for memory deallocation)
- Called from (representative examples):
  - [lo_import_internal](../l/lo_import_internal.md)
  - [be_lo_export](../b/be_lo_export.md)
  - [closeLOfd](../c/closeLOfd.md)
  - [lo_get_fragment_internal](../l/lo_get_fragment_internal.md)
  - [be_lo_from_bytea](../b/be_lo_from_bytea.md)
  - [be_lo_put](../b/be_lo_put.md)

## Notes and Other Information
- Must be called with a valid LargeObjectDesc pointer obtained from inv_open
- The function uses an assertion to validate the pointer, which will cause a failure in debug builds if passed a NULL or invalid pointer
- After calling this function, the obj_desc pointer becomes invalid and should not be used
- This is the proper way to clean up large object descriptors and prevent memory leaks
- The function does not affect the actual large object data in the database, only the local descriptor

## Simplified Source

```c
void inv_close(LargeObjectDesc *obj_desc)
{
    // Validate the descriptor pointer
    Assert(PointerIsValid(obj_desc));

    // Free the descriptor memory
    pfree(obj_desc);
}
```