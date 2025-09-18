# BrinBuildState

## Location
[src/backend/access/brin/brin.c:152-183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L152-L183)

## Overview
BrinBuildState is the primary state structure used during initial construction of a BRIN index, maintaining all necessary context for both sequential and parallel index builds.

## Definition


## Detailed Description
BrinBuildState serves as the comprehensive state container for BRIN index construction operations. It maintains both the current index building context and progress tracking information. The structure supports both sequential and parallel index builds, with specific fields dedicated to parallel coordination.

The running state during construction is kept in a BrinMemTuple (bs_dtuple), which accumulates information for the current range being processed. The structure also maintains essential metadata like pages per range, current position tracking, and access to the reverse map and index descriptor.

## Parameters / Member Variables
- : The index relation being built
- : Number of tuples processed in the current index build
- : Total number of tuples in the relation being indexed  
- : Buffer currently being used for insertions
- : Number of heap pages covered by each BRIN range
- : Block number where the current range starts
- : Maximum range start block number for the index
- : Pointer to BRIN reverse map access structure
- : Pointer to BRIN index descriptor containing opclass information
- : In-memory tuple being built for the current range
- : Template for empty BRIN tuples
- : Size of the empty tuple template
- : Memory context for build operations
- : Parallel build leader state (only present in leader process during parallel builds)
- : Worker process identifier for parallel builds
- : Tuplesort state used by workers and leader for parallel coordination

## Dependencies
- Functions called/Symbols referenced:
  - [BrinRevmap](BrinRevmap.md)
  - [BrinDesc](BrinDesc.md)
  - [BrinMemTuple](BrinMemTuple.md)
  - [BrinTuple](BrinTuple.md)
  - [BrinLeader](BrinLeader.md)
  - Tuplesortstate
- Called from (representative examples):
  - [brinbuildCallback](../b/brinbuildCallback.md)
  - [brinbuild](../b/brinbuild.md)
  - [initialize_brin_buildstate](../i/initialize_brin_buildstate.md)
  - [terminate_brin_buildstate](../t/terminate_brin_buildstate.md)
  - [summarize_range](../s/summarize_range.md)
  - [_brin_begin_parallel](../b/_brin_begin_parallel.md)
  - [_brin_parallel_heapscan](../b/_brin_parallel_heapscan.md)

## Notes and Other Information
The bs_leader field is only present during parallel index builds and only in the leader process. Worker processes do not have a BrinBuildState structure. The sortstate is used by all participants (workers and leader) in parallel builds and must be part of the build state since it's the primary structure passed to build callbacks. The structure manages both the low-level buffer operations and high-level coordination needed for efficient BRIN index construction.