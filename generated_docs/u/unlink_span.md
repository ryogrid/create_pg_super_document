# unlink_span

## Location
[src/backend/utils/mmgr/dsa.c:1906-1928](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L1906-L1928)

## Overview
Removes a span from its doubly-linked list within a fullness class, updating the appropriate pointers to maintain list integrity.

## Definition

```c
static void
unlink_span(dsa_area *area, dsa_area_span *span)
```
## Detailed Description
This function implements the standard doubly-linked list removal operation for spans within DSA fullness class lists. Each span maintains forward (nextspan) and backward (prevspan) pointers to form doubly-linked lists within each fullness class of a memory pool.

The function handles three cases: if the span has a next neighbor, it updates the next span's prevspan pointer; if the span has a previous neighbor, it updates the previous span's nextspan pointer; if the span is the head of the list (no previous span), it updates the pool's spans array to point to the next span in the list.

The operation maintains the integrity of the doubly-linked list structure that organizes spans by their fullness level, which is crucial for the DSA allocator's ability to efficiently find suitable allocation targets and manage memory fragmentation.

## Parameters / Member Variables
- : The DSA area containing the span and related structures
- : The span to be removed from its fullness class list

## Dependencies
- Functions called/Symbols referenced:
  - DsaPointerIsValid
  - [dsa_get_address](../d/dsa_get_address.md)
- Called from (representative examples):
  - [dsa_free](../d/dsa_free.md)
  - [destroy_superblock](../d/destroy_superblock.md)

## Notes and Other Information
This is a pure list manipulation function that does not involve any locking - callers must ensure appropriate synchronization. The function assumes the span is currently linked into a list and does not perform validation of the list structure. It's typically called as part of span lifecycle management when spans are moved between fullness classes or destroyed entirely.

## Simplified Source

```c
static void
unlink_span(dsa_area *area, dsa_area_span *span)
{
    // Update next span's back pointer
    if (DsaPointerIsValid(span->nextspan)) {
        dsa_area_span *next = dsa_get_address(area, span->nextspan);
        next->prevspan = span->prevspan;
    }

    // Update previous span's forward pointer OR pool head
    if (DsaPointerIsValid(span->prevspan)) {
        dsa_area_span *prev = dsa_get_address(area, span->prevspan);
        prev->nextspan = span->nextspan;
    } else {
        // This span was the head of the list, update pool's spans array
        dsa_area_pool *pool = dsa_get_address(area, span->pool);
        pool->spans[span->fclass] = span->nextspan;
    }
}
```
