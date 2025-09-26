# dsm_control_item

## Location
[src/backend/storage/ipc/dsm.c:79-87](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm.c#L79-L87)

## Overview
A shared-memory structure that maintains global state information for a single dynamic shared memory segment, coordinating its lifecycle across all PostgreSQL backend processes.

## Definition

```c
typedef struct dsm_control_item
{
	dsm_handle	handle;
	uint32		refcnt;			/* 2+ = active, 1 = moribund, 0 = gone */
	size_t		first_page;
	size_t		npages;
	void	   *impl_private_pm_handle; /* only needed on Windows */
	bool		pinned;
} dsm_control_item;
```
## Detailed Description
The  structure represents shared-memory state for a dynamic shared memory segment in PostgreSQL's DSM system. Unlike the backend-local  structure, this structure resides in shared memory and is visible to all backend processes. It serves as the authoritative source of information about a DSM segment's global state, including its reference count, physical location, and lifecycle status. The structure is part of the DSM control segment infrastructure that coordinates DSM operations across multiple backends.

## Parameters / Member Variables
- : A  that uniquely identifies this DSM segment across the entire PostgreSQL cluster
- : A  reference count that tracks the segment's lifecycle state - values of 2 or higher indicate the segment is active, 1 indicates it's moribund (marked for deletion but still in use), and 0 indicates it has been destroyed
- : A  value indicating the first page number in the underlying storage where this segment begins
- : A  value indicating the number of pages occupied by this segment
- : A  pointer to implementation-specific private data for the postmaster handle, primarily used on Windows platforms
- : A  flag indicating whether this segment is pinned in memory and should not be automatically destroyed

## Dependencies
- Functions called/Symbols referenced:
  - dsm_handle (segment identifier type)
- Called from (representative examples):
  - [dsm_control_header](dsm_control_header.md) (contains arrays of dsm_control_item structures)
  - [dsm_control_bytes_needed](dsm_control_bytes_needed.md) (calculates space needed for control items)

## Notes and Other Information
- This structure resides in the DSM control segment, which is itself a special shared memory segment used to coordinate other DSM segments
- The reference counting mechanism (refcnt) implements a simple garbage collection system for DSM segments
- The first_page and npages fields allow the system to track the physical storage layout of segments
- On Windows, the impl_private_pm_handle field stores additional handle information needed by the Windows DSM implementation
- Pinned segments (pinned = true) are protected from automatic cleanup and must be explicitly unpinned before they can be destroyed
- The structure is accessed under appropriate locking to ensure consistency across concurrent operations from multiple backends