# ResourceOwnerForgetBuffer

## Location
[src/include/storage/buf_internals.h:398-402](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/buf_internals.h#L398-L402)

## Overview
Removes a buffer pin from a resource owner's tracking list when the buffer is explicitly unpinned, completing the resource management lifecycle for buffer pins.

## Definition
```c
static inline void ResourceOwnerForgetBuffer(ResourceOwner owner, Buffer buffer)
```

## Detailed Description
ResourceOwnerForgetBuffer is a convenience wrapper function that removes a previously registered buffer pin from the resource owner's tracking system. This function is called when a buffer is explicitly unpinned (reference count decremented) to inform the resource owner that it no longer needs to track this particular buffer pin for cleanup purposes.

The function works by calling the generic ResourceOwnerForget function with the buffer converted to a Datum using Int32GetDatum, along with a reference to buffer_pin_resowner_desc which identifies the resource type. This removal ensures that the resource owner's cleanup mechanisms won't attempt to unpin the buffer again during transaction abort or error recovery scenarios.

This is the counterpart to ResourceOwnerRememberBuffer and is essential for maintaining accurate resource tracking. Without properly forgetting buffer pins when they are released normally, the resource owner would have stale references that could lead to double-unpinning or other resource management errors during cleanup operations.

## Parameters / Member Variables
- `owner`: ResourceOwner that was tracking this buffer pin and should stop tracking it
- `buffer`: Buffer identifier that has been unpinned and should be removed from tracking

## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwner (structure type)
  - ResourceOwnerForget (generic resource untracking function)
  - [Int32GetDatum](../I/Int32GetDatum.md) (conversion function)
  - buffer_pin_resowner_desc (resource descriptor for buffer pins)
- Called from (representative examples):
  - UnpinBuffer
  - UnpinLocalBuffer

## Notes and Other Information
- This function is a static inline wrapper providing type-safe buffer pin unregistration
- Must be called whenever a buffer pin count is decremented to maintain accurate resource tracking
- Part of PostgreSQL's resource owner system that prevents resource leaks and double-cleanup
- Complementary to ResourceOwnerRememberBuffer which adds the tracking when buffers are pinned
- Failure to call this function when unpinning buffers can result in resource management inconsistencies
- Essential for proper cleanup coordination between normal operation and error recovery paths
- Prevents the resource owner from attempting to unpin already-unpinned buffers during transaction cleanup