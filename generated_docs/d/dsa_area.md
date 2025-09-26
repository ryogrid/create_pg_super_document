# dsa_area

## Location
[src/backend/utils/mmgr/dsa.c:347-374](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L347-L374)

## Overview
The  struct represents the per-backend state for a dynamic shared area (DSA) storage region, tracking how segments of shared memory are mapped into a specific backend process.

## Definition

```c
struct dsa_area
{
	/* Pointer to the control object in shared memory. */
	dsa_area_control *control;

	/*
	 * All the mappings are owned by this.  The dsa_area itself is not
	 * directly tracked by the ResourceOwner, but the effect is the same. NULL
	 * if the attachment has session lifespan, i.e if dsa_pin_mapping() has
	 * been called.
	 */
	ResourceOwner resowner;

	/*
	 * This backend's array of segment maps, ordered by segment index
	 * corresponding to control->segment_handles.  Some of the area's segments
	 * may not be mapped in this backend yet, and some slots may have been
	 * freed and need to be detached; these operations happen on demand.
	 */
	dsa_segment_map segment_maps[DSA_MAX_SEGMENTS];

	/* The highest segment index this backend has ever mapped. */
	dsa_segment_index high_segment_index;

	/* The last observed freed_segment_counter. */
	size_t		freed_segment_counter;
};
```
## Detailed Description
The  struct serves as the local per-backend representation of a dynamic shared area (DSA) - a system for managing dynamically allocated shared memory segments across multiple PostgreSQL backend processes. Each backend that needs to access a DSA creates or attaches to one of these structures.

The DSA system allows backends to allocate and deallocate memory from shared memory segments that can be accessed by multiple processes. The  struct tracks which segments are mapped in the current backend and provides the interface for memory operations within the shared area.

Key aspects of the design:
- **Lazy mapping**: Segments are mapped into backends on-demand rather than all at once
- **Resource management**: Memory mappings are tied to a ResourceOwner for automatic cleanup
- **Segment tracking**: Each backend maintains its own view of which segments are mapped
- **Synchronization**: The control structure coordinates access across backends

## Parameters / Member Variables
- : Pointer to the shared  structure that coordinates the DSA across all backends
- : ResourceOwner that owns all memory mappings for this DSA attachment; NULL if the attachment has session lifespan (after )
- : Array of segment mappings ordered by segment index, corresponding to segments in the control structure; not all segments may be mapped locally
- : The highest segment index that this backend has ever mapped, used for optimization
- : Cached value of the freed segment counter to detect when segments have been freed by other backends

## Dependencies
- Functions called/Symbols referenced:
  -  (shared control structure)
  -  (resource management)
  -  (individual segment mappings)
  -  (segment indexing type)
  -  (maximum number of segments)

- Called from (representative examples):
  -  (DSA creation)
  -  (DSA attachment)
  -  (memory allocation)
  -  (memory deallocation)
  -  (address translation)
  -  (TID bitmap storage)
  -  (distributed hash tables)
  -  (TID bitmaps)

## Notes and Other Information
- Each backend process maintains its own  instance for each DSA it accesses
- The struct is defined in  at lines 347-374
- Memory segments are mapped lazily - not all segments in the DSA are necessarily mapped in every backend
- The ResourceOwner ensures that mappings are cleaned up when transactions abort or sessions end
- DSA is used extensively in parallel query execution, shared hash tables, and other shared memory scenarios
- The freed_segment_counter helps backends detect when they need to unmap segments that other backends have freed