# unlink_span

## Location
src/backend/utils/mmgr/dsa.c: 1906 - 1928

## Overview
Removes a span from its doubly-linked list within a fullness class, updating the appropriate pointers to maintain list integrity.

## Definition


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
  - dsa_get_address
- Called from (representative examples):
  - dsa_free
  - destroy_superblock

## Notes and Other Information
This is a pure list manipulation function that does not involve any locking - callers must ensure appropriate synchronization. The function assumes the span is currently linked into a list and does not perform validation of the list structure. It's typically called as part of span lifecycle management when spans are moved between fullness classes or destroyed entirely.
