# dsm_segment

## Location
[src/backend/storage/ipc/dsm.c:66-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L66-L78)

## Overview
A backend-local structure that maintains state information for a dynamic shared memory segment attached to the current PostgreSQL backend process.

## Definition

```c
struct dsm_segment
{
	dlist_node	node;			/* List link in dsm_segment_list. */
	ResourceOwner resowner;		/* Resource owner. */
	dsm_handle	handle;			/* Segment name. */
	uint32		control_slot;	/* Slot in control segment. */
	void	   *impl_private;	/* Implementation-specific private data. */
	void	   *mapped_address; /* Mapping address, or NULL if unmapped. */
	Size		mapped_size;	/* Size of our mapping. */
	slist_head	on_detach;		/* On-detach callbacks. */
};
```
## Detailed Description
The  structure represents the backend-local state for a dynamic shared memory segment in PostgreSQL. Each backend process that attaches to a DSM segment maintains one of these structures to track its relationship with that segment. The structure contains both metadata about the segment (handle, control slot) and runtime information specific to this backend's attachment (mapped address, size, callbacks). This design allows multiple backends to attach to the same underlying shared memory segment while maintaining separate local state in each backend.

## Parameters / Member Variables
- : A  that links this segment into the backend's global list of attached DSM segments ()
- : A  that tracks ownership of this DSM segment for proper cleanup during transaction or session termination
- : A  that uniquely identifies this DSM segment across the entire PostgreSQL cluster
- : A  index into the control segment that tracks global information about this DSM segment
- : A  pointer to implementation-specific private data, used by different DSM implementations (POSIX, System V, mmap, etc.)
- : A  pointer to the memory address where this segment is mapped in the current backend's address space, or NULL if not currently mapped
- : A  value indicating the size of the mapped region in bytes
- : An  that maintains a list of callback functions to be executed when this segment is detached from the current backend

## Dependencies
- Functions called/Symbols referenced:
  - dlist_node (for doubly-linked list functionality)
  - ResourceOwner (for resource ownership tracking)
  - dsm_handle (segment identifier type)
  - slist_head (for singly-linked callback list)
- Called from (representative examples):
  - dsm_create (creates and initializes new dsm_segment structures)
  - dsm_attach (attaches to existing segments and creates dsm_segment structures)
  - dsm_detach (processes and frees dsm_segment structures)
  - dsm_backend_shutdown (cleans up all dsm_segment structures during backend shutdown)

## Notes and Other Information
- This structure is purely backend-local and is not shared between different PostgreSQL backend processes
- The mapped_address can be NULL if the segment is pinned but not currently mapped into the backend's address space
- The control_slot refers to an entry in a global control segment that coordinates DSM segment lifecycle across all backends
- Different DSM implementations (POSIX shm, System V shm, mmap, Windows) use the impl_private field differently
- The on_detach callback list allows for cleanup operations to be registered and executed when the segment is detached
- Resource ownership tracking ensures that DSM segments are properly cleaned up even if a transaction or session terminates unexpectedly