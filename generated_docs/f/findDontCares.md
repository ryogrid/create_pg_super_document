# findDontCares

## Location
[src/backend/access/gist/gistsplit.c:113-166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistsplit.c#L113-L166)

## Overview
Identifies tuples that are "don't cares" - tuples that could be moved to the other side of a GiST index split with zero penalty for a specific column.

## Definition

```c
union key.
	 *
	 * attno column is known all-not-null (see gistSplitByKey), so we need not
	 * check for nulls
	 */
	gistentryinit(entry, spl->splitVector.spl_rdatum, r, NULL,
				  (OffsetNumber) 0, false);
```
## Detailed Description
This function analyzes tuples on both sides of a GiST split to find those that have zero penalty for being moved to the opposite side, specifically for the column identified by . It works by computing the penalty for each left-side tuple if added to the right-side union, and vice versa. Tuples with zero penalty are marked as "don't care" entries in the  array, meaning they can be freely reassigned to optimize the split. This is part of the GiST splitting optimization process that allows for better load balancing when some tuples are equally suitable for either side of the split.

## Parameters / Member Variables
- : Relation (table/index) being operated on
- : Pointer to GISTSTATE structure containing GiST access method information and operator class functions
- : Array of GISTENTRY structures representing the tuples being split
- : Pointer to GistSplitVector structure containing split information including:
  - : Union key for left side
  - : Union key for right side
  - : Array of indices for left-side tuples
  - : Number of left-side tuples
  - : Array of indices for right-side tuples
  - : Number of right-side tuples
  - : Output array to mark don't-care tuples (must be pre-initialized to zeros)
- : Column number being analyzed for don't-care status

## Dependencies
- Functions called/Symbols referenced:
  - [GISTSTATE](../G/GISTSTATE.md) (structure type)
  - [GISTENTRY](../G/GISTENTRY.md) (structure type)
  - [GistSplitVector](../G/GistSplitVector.md) (structure type)
  - gistentryinit (initializes GISTENTRY structures)
  - [gistpenalty](../g/gistpenalty.md) (computes penalty for adding a tuple to a union)
- Called from:
  - [gistUserPicksplit](../g/gistUserPicksplit.md) (at src/backend/access/gist/gistsplit.c:506)

## Notes and Other Information
- This is a static function, only accessible within the gistsplit.c file
- Returns the total number of don't-care tuples found
- The function assumes the specified column (attno) is all-not-null, so null checks are not performed
- Don't-care identification is used for split optimization - these tuples can be reassigned to balance the split
- The penalty function determines geometric/spatial relationships between index keys for spatial data types
- Part of the user-defined picksplit process where custom splitting strategies can leverage don't-care tuples for optimization