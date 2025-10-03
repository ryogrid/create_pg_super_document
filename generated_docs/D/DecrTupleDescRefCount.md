# DecrTupleDescRefCount

## Location
[src/backend/access/common/tupdesc.c:406-418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L406-L418)

## Overview
DecrTupleDescRefCount decrements the reference count of a TupleDesc and automatically frees it when the reference count reaches zero, providing safe memory management for reference-counted tuple descriptors.

## Definition

```c
void
DecrTupleDescRefCount(TupleDesc tupdesc)
```
## Detailed Description
This function implements the decrement operation for TupleDesc reference counting, a critical component of PostgreSQL's memory management system for tuple descriptors. When called, it:

1. Decrements the reference count () of the provided TupleDesc
2. Removes the TupleDesc reference from the CurrentResourceOwner to prevent double cleanup
3. Automatically calls FreeTupleDesc if the reference count reaches zero

The function includes an assertion to ensure the TupleDesc has a positive reference count before decrementing, helping to catch reference counting bugs during development. This function should only be used with TupleDescs that are actively being reference counted - for TupleDescs of uncertain status, the ReleaseTupleDesc macro should be used instead.

## Parameters / Member Variables
- `tupdesc`: The TupleDesc whose reference count should be decremented. Must be a valid, reference-counted TupleDesc with tdrefcount > 0.
## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerForgetTupleDesc](../R/ResourceOwnerForgetTupleDesc.md) (removes from resource owner tracking)
  - [FreeTupleDesc](../F/FreeTupleDesc.md) (deallocates when reference count reaches zero)
- Called from (representative examples):
  - [ExecEvalConvertRowtype](../E/ExecEvalConvertRowtype.md) (in expression evaluation)
  - [cache_record_field_properties](../c/cache_record_field_properties.md) (in type cache management)
  - ReleaseTupleDesc (macro wrapper for safe release)

## Notes and Other Information
- This function should only be applied to TupleDescs that are confirmed to be reference counted
- The assertion  helps detect reference counting errors in debug builds
- Resource owner cleanup occurs before the reference count check to ensure proper cleanup ordering
- When the reference count reaches zero, FreeTupleDesc is called to perform complete deallocation

## Simplified Source

```c
void
DecrTupleDescRefCount(TupleDesc tupdesc)
{
    // Ensure reference count is positive before decrementing
    Assert(tupdesc->tdrefcount > 0);

    // Remove from resource owner tracking
    ResourceOwnerForgetTupleDesc(CurrentResourceOwner, tupdesc);

    // Decrement reference count and free if it reaches zero
    if (--tupdesc->tdrefcount == 0)
        FreeTupleDesc(tupdesc);
}
```