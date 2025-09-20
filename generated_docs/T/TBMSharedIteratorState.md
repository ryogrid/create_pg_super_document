# TBMSharedIteratorState

## Location
[src/backend/nodes/tidbitmap.c:191-204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L191-L204)

## Overview
TBMSharedIteratorState is a shared memory structure that coordinates iteration state across multiple parallel processes, enabling concurrent traversal of TIDBitmap contents while maintaining consistency and thread-safe access.

## Definition

```c
typedef struct TBMSharedIteratorState
{
	int			nentries;		/* number of entries in pagetable */
	int			maxentries;		/* limit on same to meet maxbytes */
	int			npages;			/* number of exact entries in pagetable */
	int			nchunks;		/* number of lossy entries in pagetable */
	dsa_pointer pagetable;		/* dsa pointers to head of pagetable data */
	dsa_pointer spages;			/* dsa pointer to page array */
	dsa_pointer schunks;		/* dsa pointer to chunk array */
	LWLock		lock;			/* lock to protect below members */
	int			spageptr;		/* next spages index */
	int			schunkptr;		/* next schunks index */
	int			schunkbit;		/* next bit to check in current schunk */
} TBMSharedIteratorState;
```
## Detailed Description
TBMSharedIteratorState serves as the coordination mechanism for parallel TIDBitmap iteration in PostgreSQL's shared memory environment. Unlike the single-process TBMIterator, this structure enables multiple worker processes to collaboratively iterate through bitmap contents without duplicating work or missing entries.

The structure maintains both static metadata about the bitmap (nentries, npages, nchunks) and dynamic iteration state (spageptr, schunkptr, schunkbit). The dynamic state is protected by an LWLock to ensure atomic updates when multiple processes coordinate their iteration progress. DSA pointers provide shared access to the actual page and chunk data stored in dynamic shared areas.

This design is essential for parallel bitmap heap scans where multiple worker processes need to divide the work of scanning heap pages identified by bitmap index operations.

## Parameters / Member Variables
- `nentries`: Total number of entries in the shared pagetable, providing size information for iteration planning
- `maxentries`: Maximum entry limit used to meet memory constraints, inherited from the original TIDBitmap
- `npages`: Count of exact page entries available for iteration, helping balance work distribution among parallel workers
- `nchunks`: Count of lossy chunk entries available for iteration, used in work distribution calculations
- `pagetable`: DSA pointer to the head of shared pagetable data, providing access to the underlying PagetableEntry structures
- `spages`: DSA pointer to the shared array of exact page entries, sorted for efficient traversal
- `schunks`: DSA pointer to the shared array of lossy chunk entries, sorted for efficient traversal
- `lock`: LWLock protecting the iteration state variables below, ensuring atomic updates in multi-process access
- `spageptr`: Shared index into the exact pages array, coordinating which exact page the next worker should process
- `schunkptr`: Shared index into the lossy chunks array, coordinating which chunk the next worker should process
- `schunkbit`: Shared bit position within the current lossy chunk, coordinating sub-chunk processing among workers
## Dependencies
- Functions called/Symbols referenced:
  - dsa_pointer
  - [LWLock](../L/LWLock.md)
- Called from (representative examples):
  - [tbm_free_shared_area](../t/tbm_free_shared_area.md)
  - [tbm_prepare_shared_iterate](../t/tbm_prepare_shared_iterate.md)
  - [tbm_shared_iterate](../t/tbm_shared_iterate.md)
  - [tbm_attach_shared_iterate](../t/tbm_attach_shared_iterate.md)

## Notes and Other Information
- The LWLock protection is critical for maintaining consistency when multiple parallel workers coordinate their iteration progress
- DSA pointers enable efficient shared memory access across process boundaries without requiring data copying
- The structure separates static metadata (sizes, counts) from dynamic state (pointers, positions) for optimal locking granularity
- This design enables PostgreSQL's parallel bitmap heap scan functionality, significantly improving performance for large result sets
- The shared iteration state allows work to be distributed dynamically among available parallel workers
- Memory layout considerations ensure the structure can be efficiently stored and accessed in PostgreSQL's shared memory segments