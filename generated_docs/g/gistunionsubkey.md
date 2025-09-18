# gistunionsubkey

## Location
[src/backend/access/gist/gistsplit.c:80-112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistsplit.c#L80-L112)

## Overview
Recomputes union keys for both left and right sides of a GiST index page split, excluding tuples marked as "don't care" entries.

## Definition


## Detailed Description
This function updates the union keys for both sides of a GiST index page split after the split decision has been made. It calls  twice - once for the left side tuples and once for the right side tuples. The function always recomputes union keys for all index columns, even when this might represent duplicate work, because penalty functions used in splitting are not 100% accurate and a "zero penalty" doesn't guarantee that the union key remains unchanged.

## Parameters / Member Variables
- : Pointer to GISTSTATE structure containing GiST access method information and operator class functions
- : Array of IndexTuple pointers representing all tuples being split
- : Pointer to GistSplitVector structure containing split information including:
  - : Array marking tuples to ignore during union computation
  - : Array of indices for tuples assigned to left side
  - : Number of tuples assigned to left side
  - : Array of indices for tuples assigned to right side  
  - : Number of tuples assigned to right side
  - : Output array for left side union keys
  - : Output array for left side null indicators
  - : Output array for right side union keys
  - : Output array for right side null indicators

## Dependencies
- Functions called/Symbols referenced:
  - [GISTSTATE](../G/GISTSTATE.md) (structure type)
  - [GistSplitVector](../G/GistSplitVector.md) (structure type)
  - GistSplitUnion (structure type)
  - [gistunionsubkeyvec](gistunionsubkeyvec.md) (helper function called twice)
- Called from:
  - [gistUserPicksplit](gistUserPicksplit.md) (at src/backend/access/gist/gistsplit.c:543)
  - [gistSplitByKey](gistSplitByKey.md) (at src/backend/access/gist/gistsplit.c:689)
  - [gistSplitByKey](gistSplitByKey.md) (at src/backend/access/gist/gistsplit.c:777)

## Notes and Other Information
- This is a static function, only accessible within the gistsplit.c file
- The function processes both left and right sides of the split in sequence using the same helper function
- Union keys are always recomputed for all index columns to ensure accuracy despite potentially duplicated work
- Part of the GiST index splitting process that finalizes the representative keys for the new child nodes
- The dontcare mechanism allows certain tuples to be excluded from union computation without affecting the split decision