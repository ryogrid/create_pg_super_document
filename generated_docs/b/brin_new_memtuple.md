# brin_new_memtuple

## Location
src/backend/access/brin/brin_tuple.c: 482 - 510

## Overview
Creates a new BrinMemTuple from scratch and initializes it to an empty state, allocating all necessary memory structures for in-memory BRIN tuple manipulation.

## Definition
BrinMemTuple *brin_new_memtuple(BrinDesc *brdesc)

## Detailed Description
This function allocates and initializes a complete BrinMemTuple structure for in-memory processing of BRIN tuples. It calculates the required memory based on the tuple descriptor, allocates space for values, null flags, and creates a dedicated memory context for the tuple. The function ensures proper alignment and initializes the tuple to represent an empty range initially. This is the primary constructor for BrinMemTuple objects used throughout BRIN index operations.

## Parameters / Member Variables
- brdesc: Pointer to BrinDesc structure containing the tuple descriptor and metadata needed for memory allocation calculations

## Dependencies
- Functions called/Symbols referenced:
  - MAXALIGN (memory alignment macro)
  - palloc0 (PostgreSQL memory allocation)
  - palloc (PostgreSQL memory allocation)
  - AllocSetContextCreate (memory context creation)
  - brin_memtuple_initialize (tuple initialization)
  - ALLOCSET_DEFAULT_SIZES (default memory allocation sizes)
- Called from (representative examples):
  - bringetbitmap
  - initialize_brin_buildstate
  - brin_parallel_merge
  - brin_build_empty_tuple
  - brin_deform_tuple
  - BrinTupleIsEmptyRange

## Notes and Other Information
- Allocates memory in the current memory context but creates a dedicated sub-context for tuple-specific allocations
- Sets bt_empty_range to true initially, indicating an empty range state
- The function comment warns about using temporary memory contexts since no cleanup function is provided
- Memory layout includes space for BrinMemTuple structure, BrinValues array, and Datum storage
- Automatically calls brin_memtuple_initialize to complete the initialization process