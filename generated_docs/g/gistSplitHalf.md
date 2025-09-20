# gistSplitHalf

## Location
[src/backend/access/gist/gistsplit.c:585-622](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistsplit.c#L585-L622)

## Overview
Performs a simple even split of a GiST index page by dividing tuples in half between left and right sides.

## Definition

```c
union keys, caller took care of it */
}

/*
 * gistSplitByKey: main entry point for page-splitting algorithm
 *
 * r: index relation
 * page: page being split
 * itup: array of IndexTuples to be processed
 * len: number of IndexTuples to be processed (must be at least 2)
 * giststate: additional info about index
 * v: working state and output area
 * attno: column we are working on (zero-based index)
 *
 * Outside caller must initialize v->spl_lisnull and v->spl_risnull arrays
 * to all-true.  On return, spl_left/spl_nleft contain indexes of tuples
 * to go left, spl_right/spl_nright contain indexes of tuples to go right,
 * spl_lattr/spl_lisnull contain left-side union key values, and
 * spl_rattr/spl_risnull contain right-side union key values.  Other fields
 * in this struct are workspace for this file.
 *
 * Outside caller must pass zero for attno.  The function may internally
 * recurse to the next column by passing attno+1.
 */
void
gistSplitByKey(Relation r, Page page, IndexTuple *itup, int len,
			   GISTSTATE *giststate, GistSplitVector *v, int attno)
{
	GistEntryVector *entryvec;
```
## Detailed Description
This function implements the most basic splitting strategy for GiST index pages when more sophisticated methods are not applicable or have failed. It divides the tuples evenly:

1. **Memory Allocation**: Allocates memory for both left and right offset arrays based on the total number of tuples.

2. **Even Distribution**: Distributes tuples by placing the first half in the right side and the second half in the left side of the split.

3. **No Union Key Computation**: Unlike other split methods, this function does not compute union keys, leaving that responsibility to the caller.

This simple approach ensures that any page can be split when other methods fail, maintaining the basic functionality of the index structure.

## Parameters / Member Variables
- : The split vector structure to be populated with the split results
- : The total number of tuples to be split

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (for memory allocation)
- Types referenced:
  - [GIST_SPLITVEC](../G/GIST_SPLITVEC.md)
  - OffsetNumber
- Called from:
  - [gistSplitByKey](gistSplitByKey.md)

## Notes and Other Information
- This is a fallback method used when other splitting strategies are not available or appropriate
- The function assumes 1-based indexing for the tuples (starts from i=1)
- Memory allocation is performed but union key computation is deliberately omitted
- The split is deterministic and balanced, ensuring predictable behavior
- Typically used in scenarios where tuple-specific splitting methods cannot be applied
- The caller is responsible for computing appropriate union keys after the split