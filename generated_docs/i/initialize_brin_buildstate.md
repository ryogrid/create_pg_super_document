# initialize_brin_buildstate

## Location
src/backend/access/brin/brin.c: 1660 - 1706

## Overview
Initializes and allocates a BrinBuildState structure that contains all necessary state information for building BRIN index tuples.

## Definition
```c
static BrinBuildState *initialize_brin_buildstate(Relation idxRel, BrinRevmap *revmap, BlockNumber pagesPerRange, BlockNumber tablePages)
```

## Detailed Description
This static function creates and initializes a BrinBuildState structure that serves as the central state container during BRIN index construction or summarization operations. It sets up all necessary fields including the index relation, revmap access, tuple descriptors, and calculates important boundary values like the maximum range start based on the total number of table pages. The function also prepares memory contexts and initializes fields related to parallel processing.

## Parameters / Member Variables
- `idxRel`: The BRIN index relation being built or maintained
- `revmap`: Pointer to the reverse mapping structure for the BRIN index  
- `pagesPerRange`: Number of heap pages covered by each BRIN index range
- `tablePages`: Total number of pages in the table being indexed

## Dependencies
- Functions called/Symbols referenced:
  - palloc_object
  - [brin_build_desc](../b/brin_build_desc.md)
  - [brin_new_memtuple](../b/brin_new_memtuple.md)
  - CurrentMemoryContext
  - InvalidBuffer
- Types referenced:
  - [BrinBuildState](../B/BrinBuildState.md)
  - [BrinRevmap](../B/BrinRevmap.md)
  - BlockNumber
- Called from (representative examples):
  - [brinbuild](../b/brinbuild.md)
  - [brinsummarize](../b/brinsummarize.md)
  - [_brin_parallel_build_main](../b/_brin_parallel_build_main.md)

## Notes and Other Information
- This is a static function only accessible within the brin.c module
- The function calculates the maximum range start to determine when index building should stop
- Initializes fields for both serial and parallel BRIN index building
- Sets up memory context management for empty tuples that may be needed during the build process
- The bs_maxRangeStart calculation ensures proper handling of the last page range in the table