# ReleaseManyTestResource

## Location
[src/test/modules/test_resowner/test_resowner_many.c:68-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_resowner/test_resowner_many.c#L68-L82)

## Overview
A ResourceOwner callback function that properly releases ManyTestResource objects when they are no longer needed, ensuring cleanup and maintaining proper release priority ordering.

## Definition

```c
static void
ReleaseManyTestResource(Datum res)
```
## Detailed Description
ReleaseManyTestResource serves as the cleanup callback function for the PostgreSQL ResourceOwner system when dealing with ManyTestResource objects. This function is invoked automatically by the resource management system to properly deallocate test resources and maintain system integrity.

The function performs several critical operations:
1. Converts the generic Datum parameter back to a ManyTestResource pointer
2. Logs debug information about the resource being released
3. Validates that resources are released in proper priority order through an assertion
4. Removes the resource from its doubly-linked list
5. Updates release statistics for the resource kind
6. Frees the allocated memory

The priority validation ensures that the resource management system releases resources in the correct order, which is crucial for maintaining system consistency during cleanup operations.

## Parameters / Member Variables
- `res`: A Datum containing a pointer to the ManyTestResource object to be released
## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](../D/DatumGetPointer.md) (macro to extract pointer from Datum)
  - elog (logging function)
  - Assert (assertion macro)
  - [dlist_delete](../d/dlist_delete.md) (doubly-linked list deletion)
  - [pfree](../p/pfree.md) (memory deallocation)
- Called from (representative examples):
  - ManyTestResource (referenced as callback)
  - [InitManyTestResourceKind](../I/InitManyTestResourceKind.md) (registered as callback)

## Notes and Other Information
- This is a static function, only accessible within the test_resowner_many.c file
- The function maintains the global variable last_release_priority to track release ordering
- Debug logging is performed at DEBUG1 level to aid in testing and debugging
- The function assumes proper resource management discipline - resources must be released in priority order
- Memory management follows PostgreSQL conventions using pfree() for cleanup