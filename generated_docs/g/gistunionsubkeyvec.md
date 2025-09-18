# gistunionsubkeyvec

## Location
src/backend/access/gist/gistsplit.c: 47 - 79

## Overview
Forms unions of subkeys in index tuples, filtering out tuples marked as "don't care" entries during GiST index splitting operations.

## Definition


## Detailed Description
This function is a subroutine for  that processes a subset of index tuples to create union values for GiST index splitting. It creates a cleaned array of index tuples by excluding any tuples that are marked in the  array, then calls  to compute the actual union of the remaining tuples. This filtering mechanism allows the splitting algorithm to ignore certain tuples when computing representative union values for index nodes.

## Parameters / Member Variables
- : Pointer to GISTSTATE structure containing GiST access method information and operator class functions
- : Array of IndexTuple pointers representing the tuples to potentially include in the union
- : Pointer to GistSplitUnion structure containing:
  - : Array of indices specifying which tuples from itvec to consider
  - : Number of entries in the entries array
  - : Optional boolean array marking tuples to ignore (can be NULL)
  - : Attribute number being processed
  - : Output parameter indicating if the result union is null

## Dependencies
- Functions called/Symbols referenced:
  - [GISTSTATE](../G/GISTSTATE.md) (structure type)
  - GistSplitUnion (structure type)
  - [gistMakeUnionItVec](gistMakeUnionItVec.md) (function to compute union of index tuples)
  - [palloc](../p/palloc.md) (memory allocation)
  - [pfree](../p/pfree.md) (memory deallocation)
- Called from:
  - [gistunionsubkey](gistunionsubkey.md) (at src/backend/access/gist/gistsplit.c:91)
  - [gistunionsubkey](gistunionsubkey.md) (at src/backend/access/gist/gistsplit.c:98)

## Notes and Other Information
- This is a static function, only accessible within the gistsplit.c file
- The function handles memory management by allocating a temporary cleaned array and freeing it after use
- The dontcare array filtering mechanism is optional - if dontcare is NULL, all specified entries are included
- Index entries are 1-based in the entries array but converted to 0-based for itvec access
- Part of the GiST index splitting algorithm that determines how to partition tuples among child nodes