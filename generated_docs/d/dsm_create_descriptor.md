# dsm_create_descriptor

## Location
[src/backend/storage/ipc/dsm.c:1201-1236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L1201-L1236)

## Overview
Creates and initializes a new DSM segment descriptor structure, setting up the basic infrastructure needed to manage a dynamic shared memory segment.

## Definition
```c
static dsm_segment *dsm_create_descriptor(void)
```

## Detailed Description
This internal helper function allocates and initializes a new `dsm_segment` structure that serves as a descriptor for managing dynamic shared memory segments. The function performs several key initialization steps:

1. Ensures the current resource owner has sufficient capacity for tracking the new segment
2. Allocates memory for the segment descriptor in TopMemoryContext for persistence
3. Adds the new descriptor to the global DSM segment list
4. Initializes all descriptor fields to safe default values
5. Associates the descriptor with the current resource owner for proper cleanup
6. Initializes the on-detach callback list

The caller is responsible for setting the `handle` field after creation, as this function only creates the descriptor structure without establishing the actual shared memory mapping.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - ResourceOwnerEnlarge
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [dlist_push_head](dlist_push_head.md)
  - ResourceOwnerRememberDSM
  - [slist_init](../s/slist_init.md)
  - INVALID_CONTROL_SLOT
- Called from (representative examples):
  - [dsm_create](dsm_create.md)
  - [dsm_attach](dsm_attach.md)

## Notes and Other Information
- Static function - internal to dsm.c implementation
- Allocates descriptor in TopMemoryContext to ensure persistence across memory context resets
- Caller must initialize the `handle` field after creation
- Integrates with PostgreSQL's resource owner system for proper cleanup
- Located in src/backend/storage/ipc/dsm.c:1201-1236