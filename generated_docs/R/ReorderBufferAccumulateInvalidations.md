# ReorderBufferAccumulateInvalidations

## Location
[src/backend/replication/logical/reorderbuffer.c:3381-3418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L3381-L3418)

## Overview
A static helper function that accumulates invalidation messages into an existing array, handling memory allocation and reallocation as needed.

## Definition
static void ReorderBufferAccumulateInvalidations(SharedInvalidationMessage **invals_out, uint32 *ninvals_out, SharedInvalidationMessage *msgs_new, Size nmsgs_new)

## Detailed Description
This utility function manages the accumulation of invalidation messages into a dynamically growing array. If the output array is empty, it allocates initial memory and copies the new messages. If the array already contains messages, it reallocates the array to accommodate additional messages and appends the new ones. The function is designed to be called multiple times to build up a collection of invalidation messages from various sources.

## Parameters / Member Variables
- `invals_out`: Pointer to the array of accumulated invalidation messages (modified by reference)
- `ninvals_out`: Pointer to the count of accumulated messages (modified by reference)
- `msgs_new`: Array of new invalidation messages to be added
- `nmsgs_new`: Number of new messages in the msgs_new array

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - [repalloc](../r/repalloc.md)
  - memcpy
- Called from (representative examples):
  - [ReorderBufferAddInvalidations](ReorderBufferAddInvalidations.md)
  - [ReorderBufferAddDistributedInvalidations](ReorderBufferAddDistributedInvalidations.md)

## Notes and Other Information
- This is a static (internal) function not exposed outside the reorderbuffer.c file
- Handles both initial allocation (when *ninvals_out == 0) and reallocation scenarios
- Uses repalloc for efficient memory reallocation when extending the array
- The function modifies its output parameters by reference using double pointers
- Memory management is handled automatically - caller doesn't need to pre-allocate

## Simplified Source

```c
static void
ReorderBufferAccumulateInvalidations(SharedInvalidationMessage **invals_out,
                                    uint32 *ninvals_out,
                                    SharedInvalidationMessage *msgs_new,
                                    Size nmsgs_new)
{
    if (*ninvals_out == 0) {
        // First time: allocate new array and copy messages
        *ninvals_out = nmsgs_new;
        *invals_out = palloc(sizeof(SharedInvalidationMessage) * nmsgs_new);
        memcpy(*invals_out, msgs_new, sizeof(SharedInvalidationMessage) * nmsgs_new);
    }
    else {
        // Extend existing array: reallocate and append new messages
        *invals_out = repalloc(*invals_out,
                              sizeof(SharedInvalidationMessage) * (*ninvals_out + nmsgs_new));
        memcpy(*invals_out + *ninvals_out, msgs_new,
               nmsgs_new * sizeof(SharedInvalidationMessage));
        *ninvals_out += nmsgs_new;
    }
}
```