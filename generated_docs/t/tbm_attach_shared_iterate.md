# tbm_attach_shared_iterate

## Location
src/backend/nodes/tidbitmap.c: 1461 - 1493

## Overview
Allocates a backend-private iterator and attaches it to shared iterator state, enabling multiple processes to iterate jointly over a shared TID bitmap.

## Definition


## Detailed Description
This function creates a backend-private TBMSharedIterator that connects to shared iterator state stored in dynamic shared memory. It converts DSA (Dynamic Shared Area) pointers to local pointers for efficient access during iteration. The function allocates sufficient memory for the iterator structure plus trailing space for tuple offset numbers, then maps the shared state components (pagetable, spages, schunks) to local address space for the current backend process.

## Parameters / Member Variables
- `dsa`: Pointer to the dynamic shared memory area containing the shared iterator state
- `dp`: DSA pointer to the shared iterator state structure

## Dependencies
- Functions called/Symbols referenced:
  - dsa_area (type)
  - dsa_pointer (type)
  - TBMSharedIterator (struct type)
  - TBMSharedIteratorState (struct type)
  - MAX_TUPLES_PER_PAGE (constant)
  - dsa_get_address (function)
  - palloc0 (function)
- Called from (representative examples):
  - BitmapHeapNext

## Notes and Other Information
- Supports parallel bitmap heap scans by allowing multiple workers to share iteration state
- Converts shared memory pointers to local pointers for performance
- Allocates trailing space for MAX_TUPLES_PER_PAGE offset numbers
- Essential component of PostgreSQL's parallel query execution for bitmap scans
- Returns a fully initialized iterator ready for use with tbm_shared_iterate