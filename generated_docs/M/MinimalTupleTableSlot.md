# MinimalTupleTableSlot

## Location
src/include/executor/tuptable.h: 282 - 301

## Overview
MinimalTupleTableSlot is a specialized table slot structure designed to handle MinimalTuple objects, providing an interface that allows minimal tuples to be accessed using the same column extraction mechanisms as regular heap tuples.

## Definition


## Detailed Description
MinimalTupleTableSlot is a specialized tuple table slot implementation designed specifically for handling MinimalTuple objects. MinimalTuples are a compact representation of tuples that contain only the essential data without the full HeapTuple header overhead, making them more memory-efficient for operations like sorting and temporary storage.

The key design principle is to provide a unified interface that allows minimal tuples to be accessed using the same column extraction mechanisms as regular heap tuples. This is achieved through a clever arrangement where the tuple field points to the minhdr structure, which is configured to make the minimal tuple appear like a regular heap tuple for access purposes.

The minhdr.t_data field is set to point MINIMAL_TUPLE_OFFSET bytes before the actual mintuple data, creating the illusion of a standard heap tuple header. This allows the column extraction code (slot_deform_heap_tuple) to work identically for both regular and minimal tuples without requiring separate code paths.

The off field stores saved state information used during tuple deformation, allowing efficient incremental column access by remembering where the previous deformation operation left off.

## Parameters / Member Variables
- : Base TupleTableSlot structure containing common slot functionality
- : HeapTuple wrapper that points to minhdr for unified access interface
- : Pointer to the actual MinimalTuple data, or NULL if no tuple is stored
- : HeapTupleData workspace structure configured to make minimal tuple appear like a regular heap tuple
- : Saved offset state for efficient incremental tuple deformation operations

## Dependencies
- Functions called/Symbols referenced:
  - TupleTableSlot
  - HeapTuple
  - MinimalTuple
  - HeapTupleData
  - MINIMAL_TUPLE_OFFSET (constant)

- Called from (representative examples):
  - tts_minimal_init
  - tts_minimal_clear
  - tts_minimal_getsomeattrs
  - tts_minimal_materialize
  - slot_deform_heap_tuple

## Notes and Other Information
- Used primarily for efficient storage and manipulation of temporary tuples during sorting, hashing, and other operations
- The clever header arrangement allows seamless integration with existing tuple access mechanisms
- MinimalTuples are more compact than regular HeapTuples, saving memory in operations that handle large numbers of tuples
- The FIELDNO_* definitions are used for efficient field access in performance-critical code paths
- Essential component of PostgreSQL's tuple table slot framework, providing specialized handling for minimal tuple format
- The off field enables efficient incremental column access, avoiding redundant deformation work