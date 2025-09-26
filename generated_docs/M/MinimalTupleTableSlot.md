# MinimalTupleTableSlot

## Location
[src/include/executor/tuptable.h:282-301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/tuptable.h#L282-L301)

## Overview
MinimalTupleTableSlot is a specialized table slot structure designed to handle MinimalTuple objects, providing an interface that allows minimal tuples to be accessed using the same column extraction mechanisms as regular heap tuples.

## Definition

```c
typedef struct MinimalTupleTableSlot
{
	pg_node_attr(abstract)

	TupleTableSlot base;

	/*
	 * In a minimal slot tuple points at minhdr and the fields of that struct
	 * are set correctly for access to the minimal tuple; in particular,
	 * minhdr.t_data points MINIMAL_TUPLE_OFFSET bytes before mintuple.  This
	 * allows column extraction to treat the case identically to regular
	 * physical tuples.
	 */
#define FIELDNO_MINIMALTUPLETABLESLOT_TUPLE 1
	HeapTuple	tuple;			/* tuple wrapper */
	MinimalTuple mintuple;		/* minimal tuple, or NULL if none */
	HeapTupleData minhdr;		/* workspace for minimal-tuple-only case */
#define FIELDNO_MINIMALTUPLETABLESLOT_OFF 4
	uint32		off;			/* saved state for slot_deform_heap_tuple */
} MinimalTupleTableSlot;
```
## Detailed Description
MinimalTupleTableSlot is a specialized tuple table slot implementation designed specifically for handling MinimalTuple objects. MinimalTuples are a compact representation of tuples that contain only the essential data without the full HeapTuple header overhead, making them more memory-efficient for operations like sorting and temporary storage.

The key design principle is to provide a unified interface that allows minimal tuples to be accessed using the same column extraction mechanisms as regular heap tuples. This is achieved through a clever arrangement where the tuple field points to the minhdr structure, which is configured to make the minimal tuple appear like a regular heap tuple for access purposes.

The minhdr.t_data field is set to point MINIMAL_TUPLE_OFFSET bytes before the actual mintuple data, creating the illusion of a standard heap tuple header. This allows the column extraction code (slot_deform_heap_tuple) to work identically for both regular and minimal tuples without requiring separate code paths.

The off field stores saved state information used during tuple deformation, allowing efficient incremental column access by remembering where the previous deformation operation left off.

## Parameters / Member Variables
- `base`: Base TupleTableSlot structure containing common slot functionality
- `tuple`: HeapTuple wrapper that points to minhdr for unified access interface
- `mintuple`: Pointer to the actual MinimalTuple data, or NULL if no tuple is stored
- `minhdr`: HeapTupleData workspace structure configured to make minimal tuple appear like a regular heap tuple
- `off`: Saved offset state for efficient incremental tuple deformation operations
## Dependencies
- Functions called/Symbols referenced:
  - [TupleTableSlot](../T/TupleTableSlot.md)
  - HeapTuple
  - MinimalTuple
  - [HeapTupleData](../H/HeapTupleData.md)
  - MINIMAL_TUPLE_OFFSET (constant)

- Called from (representative examples):
  - [tts_minimal_init](../t/tts_minimal_init.md)
  - [tts_minimal_clear](../t/tts_minimal_clear.md)
  - [tts_minimal_getsomeattrs](../t/tts_minimal_getsomeattrs.md)
  - [tts_minimal_materialize](../t/tts_minimal_materialize.md)
  - [slot_deform_heap_tuple](../s/slot_deform_heap_tuple.md)

## Notes and Other Information
- Used primarily for efficient storage and manipulation of temporary tuples during sorting, hashing, and other operations
- The clever header arrangement allows seamless integration with existing tuple access mechanisms
- MinimalTuples are more compact than regular HeapTuples, saving memory in operations that handle large numbers of tuples
- The FIELDNO_* definitions are used for efficient field access in performance-critical code paths
- Essential component of PostgreSQL's tuple table slot framework, providing specialized handling for minimal tuple format
- The off field enables efficient incremental column access, avoiding redundant deformation work