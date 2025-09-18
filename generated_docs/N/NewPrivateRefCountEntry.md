# NewPrivateRefCountEntry

## Location
src/backend/storage/buffer/bufmgr.c: 315 - 340

## Overview
NewPrivateRefCountEntry fills a previously reserved refcount entry with buffer information, initializing tracking for a buffer's reference count.

## Definition
```c
static PrivateRefCountEntry *NewPrivateRefCountEntry(Buffer buffer)
```

## Detailed Description
This function consumes a previously reserved entry from the private reference count tracking system and initializes it for a specific buffer. It's designed to work in conjunction with ReservePrivateRefCountEntry(), which must be called first to ensure space availability.

The function takes the reserved entry (stored in ReservedRefCountEntry), clears the reservation, and initializes the entry with the provided buffer ID and a reference count of 0. This establishes the foundation for tracking how many times the buffer is pinned by the current backend.

## Parameters / Member Variables
- `buffer`: The Buffer ID to be tracked in the new reference count entry

## Dependencies
- Functions called/Symbols referenced:
  - PrivateRefCountEntry (struct type)
  - ReservedRefCountEntry (global variable)
- Called from (representative examples):
  - PinBuffer
  - PinBuffer_Locked

## Notes and Other Information
- Must only be called when a reservation has been made via ReservePrivateRefCountEntry()
- The function includes an assertion to ensure ReservedRefCountEntry is not NULL
- Initializes the refcount to 0, expecting the caller to increment it as needed
- Part of PostgreSQL's buffer pinning mechanism that prevents buffers from being evicted while in use
- The returned entry can be used to track and modify the reference count for the specified buffer