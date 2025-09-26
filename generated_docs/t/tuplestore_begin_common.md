# tuplestore_begin_common

## Location
src/backend/utils/sort/tuplestore.c: 253 - 317

## Overview
Internal common initialization function for creating a new tuplestore operation, setting up the basic data structures and memory management for tuple storage.

## Definition


## Detailed Description
This is the core initialization function used by all tuplestore_begin_xxx variants to create and configure a new Tuplestorestate. It allocates and initializes the main tuplestore data structure with default values, sets up memory management limits, and creates the initial read pointer array. The function establishes the tuplestore in TSS_INMEM status, meaning tuples will initially be stored in memory before potentially being spilled to disk when memory limits are exceeded.

The function calculates an appropriate initial size for the memory tuple array based on ALLOCSET_SEPARATE_THRESHOLD to ensure efficient memory allocation patterns. It also sets up a single default read pointer at position 0.

## Parameters / Member Variables
- : Execution flags indicating the capabilities required (e.g., backward scan support)
- : Boolean flag indicating whether the tuplestore should survive transaction boundaries
- : Maximum memory allowed for the tuplestore in kilobytes before spilling to disk

## Dependencies
- Functions called/Symbols referenced:
  - palloc0 (memory allocation)
  - palloc (memory allocation)
  - GetMemoryChunkSpace (memory tracking)
  - USEMEM (memory accounting macro)
  - Max (macro for maximum value)
- Data structures used:
  - Tuplestorestate (main tuplestore state structure)
  - TSReadPointer (read pointer structure)
  - TSS_INMEM (tuplestore status enum value)
  - ALLOCSET_SEPARATE_THRESHOLD (memory allocation constant)
- Called from:
  - tuplestore_begin_heap (heap tuple variant initialization)

## Notes and Other Information
- This is a static function, only accessible within tuplestore.c
- The initial memtuples array size is carefully calculated to work well with PostgreSQL's memory allocator
- Sets up exactly one read pointer initially, but the array can grow to accommodate multiple read pointers
- Memory accounting begins immediately with USEMEM tracking the initial memtuples allocation
- The backward scan capability and function pointers for tuple operations are not set here - they are configured by the calling tuplestore_begin_xxx functions