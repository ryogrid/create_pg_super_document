# gistunion

## Location
[src/backend/access/gist/gistutil.c:218-231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistutil.c#L218-L231)

## Overview
Creates an IndexTuple containing union keys by applying GiST union operations to a vector of IndexTuples and formatting the results into a new tuple.

## Definition
IndexTuple gistunion(Relation r, IndexTuple *itvec, int len, GISTSTATE *giststate)

## Detailed Description
This is a high-level convenience function that combines the union creation and tuple formation processes. It takes a vector of IndexTuples, computes union datums for each column using gistMakeUnionItVec, and then formats these union datums into a proper IndexTuple using gistFormTuple. The resulting IndexTuple represents a bounding key that encompasses all the input tuples, making it suitable for use as an internal node key in the GiST index structure.

The function serves as a bridge between the lower-level union computation (gistMakeUnionItVec) and tuple formation (gistFormTuple), providing a single interface for creating union keys from multiple IndexTuples.

## Parameters / Member Variables
- `r`: Relation (index) for which the union tuple is being created
- `itvec`: Array of IndexTuple pointers to be combined into a union
- `len`: Number of IndexTuples in the itvec array
- `giststate`: GIST state structure containing index metadata and function pointers

## Dependencies
- Functions called/Symbols referenced:
  - [GISTSTATE](../G/GISTSTATE.md) (GiST state structure type)
  - INDEX_MAX_KEYS (maximum number of index columns constant)
  - [gistMakeUnionItVec](gistMakeUnionItVec.md) (computes union datums for each column)
  - gistFormTuple (creates IndexTuple from datum arrays)
- Called from (representative examples):
  - [gist_indexsortbuild_levelstate_flush](gist_indexsortbuild_levelstate_flush.md) (in gistbuild.c:530)

## Notes and Other Information
- This function is a simple wrapper that combines two fundamental GiST operations: union computation and tuple formation
- The attr and isnull arrays use INDEX_MAX_KEYS size to handle the maximum possible number of index columns
- The resulting IndexTuple is returned uncompressed (false parameter to gistFormTuple)
- This function is primarily used during index construction when building internal nodes from leaf-level data
- The function assumes valid input parameters and relies on the underlying functions for error handling