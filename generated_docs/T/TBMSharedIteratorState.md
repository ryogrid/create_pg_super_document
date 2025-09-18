# TBMSharedIteratorState

## Location
src/backend/nodes/tidbitmap.c: 191 - 204

## Overview
TBMSharedIteratorState is a shared memory structure that coordinates iteration state across multiple parallel processes, enabling concurrent traversal of TIDBitmap contents while maintaining consistency and thread-safe access.

## Definition


## Detailed Description
TBMSharedIteratorState serves as the coordination mechanism for parallel TIDBitmap iteration in PostgreSQL's shared memory environment. Unlike the single-process TBMIterator, this structure enables multiple worker processes to collaboratively iterate through bitmap contents without duplicating work or missing entries.

The structure maintains both static metadata about the bitmap (nentries, npages, nchunks) and dynamic iteration state (spageptr, schunkptr, schunkbit). The dynamic state is protected by an LWLock to ensure atomic updates when multiple processes coordinate their iteration progress. DSA pointers provide shared access to the actual page and chunk data stored in dynamic shared areas.

This design is essential for parallel bitmap heap scans where multiple worker processes need to divide the work of scanning heap pages identified by bitmap index operations.

## Parameters / Member Variables
- : Total number of entries in the shared pagetable, providing size information for iteration planning
- : Maximum entry limit used to meet memory constraints, inherited from the original TIDBitmap
- : Count of exact page entries available for iteration, helping balance work distribution among parallel workers
- : Count of lossy chunk entries available for iteration, used in work distribution calculations
- : DSA pointer to the head of shared pagetable data, providing access to the underlying PagetableEntry structures
- : DSA pointer to the shared array of exact page entries, sorted for efficient traversal
- : DSA pointer to the shared array of lossy chunk entries, sorted for efficient traversal
- : LWLock protecting the iteration state variables below, ensuring atomic updates in multi-process access
- : Shared index into the exact pages array, coordinating which exact page the next worker should process
- : Shared index into the lossy chunks array, coordinating which chunk the next worker should process
- : Shared bit position within the current lossy chunk, coordinating sub-chunk processing among workers

## Dependencies
- Functions called/Symbols referenced:
  - dsa_pointer
  - LWLock
- Called from (representative examples):
  - tbm_free_shared_area
  - tbm_prepare_shared_iterate
  - tbm_shared_iterate
  - tbm_attach_shared_iterate

## Notes and Other Information
- The LWLock protection is critical for maintaining consistency when multiple parallel workers coordinate their iteration progress
- DSA pointers enable efficient shared memory access across process boundaries without requiring data copying
- The structure separates static metadata (sizes, counts) from dynamic state (pointers, positions) for optimal locking granularity
- This design enables PostgreSQL's parallel bitmap heap scan functionality, significantly improving performance for large result sets
- The shared iteration state allows work to be distributed dynamically among available parallel workers
- Memory layout considerations ensure the structure can be efficiently stored and accessed in PostgreSQL's shared memory segments