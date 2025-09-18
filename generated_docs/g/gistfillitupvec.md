# gistfillitupvec

## Location
src/backend/access/gist/gistutil.c: 126 - 153

## Overview
Creates a contiguous memory block containing a serialized array of IndexTuples from a vector of IndexTuple pointers, used for efficient storage and processing in GiST index operations.

## Definition
IndexTupleData *gistfillitupvec(IndexTuple *vec, int veclen, int *memlen)

## Detailed Description
This utility function takes an array of IndexTuple pointers and creates a single contiguous memory block containing all the IndexTuples in sequence. The function calculates the total memory required by summing the sizes of all IndexTuples, allocates a single memory block, and then copies each IndexTuple into the block sequentially. This compact representation is useful for operations that need to process multiple IndexTuples efficiently or pass them to other functions as a single unit.

The function also returns the total memory length through the memlen parameter, which is essential for consumers that need to know the size of the allocated block.

## Parameters / Member Variables
- `vec`: Array of IndexTuple pointers to be serialized into a contiguous block
- `veclen`: Number of IndexTuples in the vec array
- `memlen`: Output parameter that receives the total size in bytes of the allocated memory block

## Dependencies
- Functions called/Symbols referenced:
  - IndexTupleSize (macro to get the size of an IndexTuple)
  - [IndexTupleData](../I/IndexTupleData.md) (type definition for index tuple data structure)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - memcpy (standard C memory copy function)
- Called from (representative examples):
  - [gistplacetopage](gistplacetopage.md) (in gist.c:396)
  - [gistSplit](gistSplit.md) (in gist.c:1499, 1521) 
  - [gist_indexsortbuild_levelstate_flush](gist_indexsortbuild_levelstate_flush.md) (in gistbuild.c:533)

## Notes and Other Information
- The function allocates memory using palloc(), so the returned memory will be automatically freed when the current memory context is destroyed
- The returned pointer is cast to IndexTupleData* but points to a contiguous block containing multiple IndexTuples
- This function is primarily used during GiST index construction and split operations where multiple IndexTuples need to be processed as a unit
- The caller is responsible for understanding that the returned pointer points to a sequence of IndexTuples, not a single IndexTuple