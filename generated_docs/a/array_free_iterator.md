# array_free_iterator

## Location
[src/backend/utils/adt/arrayfuncs.c:4747-4768](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L4747-L4768)

## Overview
This function releases memory allocated for an ArrayIterator and its associated workspace, preventing memory leaks after array iteration is complete.

## Definition

```c
void
array_free_iterator(ArrayIterator iterator)
```
## Detailed Description
The  function performs cleanup of an ArrayIterator object created by . It conditionally frees workspace arrays that were allocated for slice-based iteration (when slice_ndim > 0), then frees the iterator structure itself. This function is essential for proper memory management when using the array iteration API.

## Parameters / Member Variables
- `iterator`: The ArrayIterator object to be freed and deallocated
## Dependencies
- Functions called/Symbols referenced:
  - : PostgreSQL memory deallocation function for freeing allocated memory
- Called from (representative examples):
  - : After completing element position search
  - : After completing search for all element positions

## Notes and Other Information
- Must be called for every iterator created with  to prevent memory leaks
- Automatically handles both scalar iteration (slice_ndim == 0) and slice iteration (slice_ndim > 0) cleanup
- Only frees slice workspace arrays if they were allocated (when slice_ndim > 0)
- The function is safe to call even if the iterator was not fully consumed
- Does not affect the original array that was being iterated over
- Should be called in exception handling paths to ensure cleanup even when iteration is interrupted