# ResOwnerReleaseTupleDesc

## Location
[src/backend/access/common/tupdesc.c:923-933](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L923-L933)

## Overview
A ResourceOwner callback function that decrements the reference count of a tuple descriptor and frees it when the count reaches zero.

## Definition
```c
static void ResOwnerReleaseTupleDesc(Datum res)
```

## Detailed Description
ResOwnerReleaseTupleDesc is a callback function used by PostgreSQL's resource management system to automatically release tuple descriptor references when a ResourceOwner is cleaned up. This function is part of the resource cleanup mechanism that ensures proper memory management and prevents resource leaks.

The function operates similarly to DecrTupleDescRefCount but with a crucial difference: it does not call ResourceOwnerForget() because the ResourceOwner system itself is handling the cleanup. It simply decrements the tuple descriptor's reference count and, if the count reaches zero, calls FreeTupleDesc to deallocate the memory.

This callback is registered with the ResourceOwner system through the tupdesc_resowner_desc structure and is automatically invoked during resource cleanup phases, specifically during the RESOURCE_RELEASE_AFTER_LOCKS phase with RELEASE_PRIO_TUPDESC_REFS priority.

## Parameters / Member Variables
- `res`: A Datum containing a pointer to the TupleDesc to be released (cast from pointer using DatumGetPointer)

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](../D/DatumGetPointer.md) (macro to extract pointer from Datum)
  - [FreeTupleDesc](../F/FreeTupleDesc.md) (deallocates tuple descriptor memory)
- Called from (representative examples):
  - PostgreSQL ResourceOwner cleanup system (automatically during resource cleanup)
  - Registered via tupdesc_resowner_desc callback structure

## Notes and Other Information
- This is a static function, only accessible within the tupdesc.c file
- Part of PostgreSQL's automatic resource management system
- Does not call ResourceOwnerForget() unlike the regular DecrTupleDescRefCount function
- Executed during the RESOURCE_RELEASE_AFTER_LOCKS phase of resource cleanup
- Has RELEASE_PRIO_TUPDESC_REFS priority in the cleanup order
- Ensures that tuple descriptors are properly freed even if manual cleanup is missed
- Maintains the assertion that tdrefcount > 0 before decrementing, ensuring reference count integrity