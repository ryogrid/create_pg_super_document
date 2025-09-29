# ForgetPrivateRefCountEntry

## Location
[src/backend/storage/buffer/bufmgr.c:438-472](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L438-L472)

## Overview
ForgetPrivateRefCountEntry releases resources used to track the reference count of a buffer that is no longer pinned and won't be pinned again immediately.

## Definition
```c
static void ForgetPrivateRefCountEntry(PrivateRefCountEntry *ref)
```

## Detailed Description
This function performs cleanup when a buffer's reference count drops to zero and the tracking entry is no longer needed. It handles both array-based and hash table-based entries differently to maintain optimal performance characteristics.

For entries stored in the PrivateRefCountArray, the function marks the buffer as invalid and optimistically reserves the slot for future use. This reservation strategy helps avoid costly searches for free entries in subsequent operations.

For entries stored in the hash table (overflow entries), the function removes them completely and decrements the overflow counter. This helps maintain accurate statistics about hash table usage and potentially allows future lookups to skip hash table searches entirely.

## Parameters / Member Variables
- `ref`: Pointer to the PrivateRefCountEntry to be forgotten/released

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md)
  - HASH_REMOVE
  - [PrivateRefCountEntry](../P/PrivateRefCountEntry.md) (struct type)
  - REFCOUNT_ARRAY_ENTRIES (macro)
- Called from (representative examples):
  - [UnpinBufferNoOwner](../U/UnpinBufferNoOwner.md)

## Notes and Other Information
- The function includes an assertion ensuring the refcount is 0 before forgetting
- Array entries are optimistically reserved for future use rather than just marked free
- [Hash](../H/Hash.md) table entries are completely removed to reduce memory usage
- Properly maintains the PrivateRefCountOverflowed counter for accurate overflow tracking
- The different handling strategies optimize for the common case of array-based storage
- Essential for preventing memory leaks in the private reference counting system
- Part of the buffer unpinning process that ensures proper resource cleanup

## Simplified Source

```c
static void
ForgetPrivateRefCountEntry(PrivateRefCountEntry *ref)
{
    Assert(ref->refcount == 0);

    // Check if this is an array entry (fast path)
    if (ref >= &PrivateRefCountArray[0] &&
        ref < &PrivateRefCountArray[REFCOUNT_ARRAY_ENTRIES])
    {
        // Mark buffer as invalid and reserve for future use
        ref->buffer = InvalidBuffer;
        ReservedRefCountEntry = ref;
    }
    else
    {
        // Hash table entry - remove completely
        bool found;
        Buffer buffer = ref->buffer;

        hash_search(PrivateRefCountHash, &buffer, HASH_REMOVE, &found);
        Assert(found);
        Assert(PrivateRefCountOverflowed > 0);
        PrivateRefCountOverflowed--;
    }
}
```