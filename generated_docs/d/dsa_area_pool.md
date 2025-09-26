# dsa_area_pool

## Location
[src/backend/utils/mmgr/dsa.c:281-318](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L281-L318)

## Overview
The dsa_area_pool structure represents a set of objects of a given size class in PostgreSQL's Dynamic Shared Memory Areas (DSA) allocator. It manages memory allocation and organization within specific size classes using spans organized by fullness levels.

## Definition

```c
typedef struct
{
	/* The segment header for the first segment. */
	dsa_segment_header segment_header;
	/* The handle for this area. */
	dsa_handle	handle;
	/* The handles of the segments owned by this area. */
	dsm_handle	segment_handles[DSA_MAX_SEGMENTS];
	/* Lists of segments, binned by maximum contiguous run of free pages. */
	dsa_segment_index segment_bins[DSA_NUM_SEGMENT_BINS];
	/* The object pools for each size class. */
	dsa_area_pool pools[DSA_NUM_SIZE_CLASSES];
	/* initial allocation segment size */
	size_t		init_segment_size;
	/* maximum allocation segment size */
	size_t		max_segment_size;
	/* The total size of all active segments. */
	size_t		total_segment_size;
	/* The maximum total size of backing storage we are allowed. */
	size_t		max_total_segment_size;
	/* Highest used segment index in the history of this area. */
	dsa_segment_index high_segment_index;
	/* The reference count for this area. */
	int			refcnt;
	/* A flag indicating that this area has been pinned. */
	bool		pinned;
	/* The number of times that segments have been freed. */
	size_t		freed_segment_counter;
	/* The LWLock tranche ID. */
	int			lwlock_tranche_id;
	/* The general lock (protects everything except object pools). */
	LWLock		lock;
} dsa_area_control;
```
## Detailed Description
The dsa_area_pool is a fundamental component of PostgreSQL's DSA memory management system. Each pool manages objects of a specific size class, organizing them into spans (contiguous regions of memory) that are categorized by their fullness level. This design enables efficient allocation and deallocation of similarly-sized objects while minimizing fragmentation.

The pool uses a fullness-based binning strategy where spans are organized into different fullness classes (typically quartiles), allowing the allocator to quickly find appropriate spans for new allocations. The pool being used for active allocations is always maintained at the head of the fullness class 1 list, regardless of its actual fullness level.

Each dsa_area_pool instance is part of a larger dsa_area_control structure, where multiple pools handle different size classes within the same DSA area. This hierarchical organization allows for efficient memory management across various object sizes.

## Parameters / Member Variables
- : An LWLock that provides thread-safe access to this specific pool, protecting concurrent operations on the pool's data structures
- : An array of dsa_pointer values representing linked lists of spans organized by fullness level (DSA_FULLNESS_CLASSES = 4), where each fullness class corresponds to different quartiles of span occupancy

## Dependencies
- Functions called/Symbols referenced:
  - LWLock
  - dsa_pointer
  - DSA_FULLNESS_CLASSES
- Called from (representative examples):
  - get_segment_index (src/backend/utils/mmgr/dsa.c:383, 385, 388)
  - dsa_allocate_extended (src/backend/utils/mmgr/dsa.c:697)
  - dsa_trim (src/backend/utils/mmgr/dsa.c:1053)
  - init_span (src/backend/utils/mmgr/dsa.c:1379)
  - transfer_first_span (src/backend/utils/mmgr/dsa.c:1433)
  - alloc_object (src/backend/utils/mmgr/dsa.c:1474)
  - ensure_active_superblock (src/backend/utils/mmgr/dsa.c:1560)
  - unlink_span (src/backend/utils/mmgr/dsa.c:1922)
  - add_span_to_fullness_class (src/backend/utils/mmgr/dsa.c:1933)

## Notes and Other Information
- The structure includes a comment about potential cacheline boundary padding, indicating performance considerations in the design
- The comment suggests that multiple pools per size class could be implemented for contention avoidance, but currently only one pool per size class is used
- This structure is part of PostgreSQL's sophisticated memory management system designed for shared memory scenarios where multiple processes need efficient, coordinated access to memory pools
- The fullness-based organization helps minimize memory fragmentation and improves allocation performance by quickly identifying suitable spans for new allocations
- Each pool operates independently with its own lock, allowing for fine-grained concurrency control in multi-process environments