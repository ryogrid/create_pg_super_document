# gistSplitByKey

## Location
[src/backend/access/gist/gistsplit.c:623-779](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistsplit.c#L623-L779)

## Overview
Main entry point for the GiST page-splitting algorithm that recursively processes index columns to optimize splits and handle complex multi-column scenarios.

## Definition

```c
union keys, unless outer recursion level will handle it */
		if (attno == 0 && giststate->nonLeafTupdesc->natts == 1)
		{
			v->spl_dontcare = NULL;
			gistunionsubkey(giststate, itup, v);
		}
	}
	else
	{
		/*
		 * All keys are not-null, so apply user-defined PickSplit method
		 */
		if (gistUserPicksplit(r, entryvec, attno, v, itup, len, giststate))
		{
			/*
			 * Splitting on attno column is not optimal, so consider
			 * redistributing don't-care tuples according to the next column
			 */
			Assert(attno + 1 < giststate->nonLeafTupdesc->natts);

			if (v->spl_dontcare == NULL)
			{
				/*
				 * This split was actually degenerate, so ignore it altogether
				 * and just split according to the next column.
				 */
				gistSplitByKey(r, page, itup, len, giststate, v, attno + 1);
			}
			else
			{
				/*
				 * Form an array of just the don't-care tuples to pass to a
				 * recursive invocation of this function for the next column.
				 */
				IndexTuple *newitup = (IndexTuple *) palloc(len * sizeof(IndexTuple));
				OffsetNumber *map = (OffsetNumber *) palloc(len * sizeof(OffsetNumber));
				int			newlen = 0;
				GIST_SPLITVEC backupSplit;

				for (i = 0; i < len; i++)
				{
					if (v->spl_dontcare[i + 1])
					{
						newitup[newlen] = itup[i];
						map[newlen] = i + 1;
						newlen++;
					}
				}

				Assert(newlen > 0);

				/*
				 * Make a backup copy of v->splitVector, since the recursive
				 * call will overwrite that with its own result.
				 */
				backupSplit = v->splitVector;
				backupSplit.spl_left = (OffsetNumber *) palloc(sizeof(OffsetNumber) * len);
				memcpy(backupSplit.spl_left, v->splitVector.spl_left, sizeof(OffsetNumber) * v->splitVector.spl_nleft);
				backupSplit.spl_right = (OffsetNumber *) palloc(sizeof(OffsetNumber) * len);
				memcpy(backupSplit.spl_right, v->splitVector.spl_right, sizeof(OffsetNumber) * v->splitVector.spl_nright);

				/* Recursively decide how to split the don't-care tuples */
				gistSplitByKey(r, page, newitup, newlen, giststate, v, attno + 1);

				/* Merge result of subsplit with non-don't-care tuples */
				for (i = 0; i < v->splitVector.spl_nleft; i++)
					backupSplit.spl_left[backupSplit.spl_nleft++] = map[v->splitVector.spl_left[i] - 1];
				for (i = 0; i < v->splitVector.spl_nright; i++)
					backupSplit.spl_right[backupSplit.spl_nright++] = map[v->splitVector.spl_right[i] - 1];

				v->splitVector = backupSplit;
			}
		}
	}

	/*
	 * If we're handling a multicolumn index, at the end of the recursion
	 * recompute the left and right union datums for all index columns.  This
	 * makes sure we hand back correct union datums in all corner cases,
	 * including when we haven't processed all columns to start with, or when
	 * a secondary split moved "don't care" tuples from one side to the other
	 * (we really shouldn't assume that that didn't change the union datums).
	 *
	 * Note: when we're in an internal recursion (attno > 0), we do not worry
	 * about whether the union datums we return with are sensible, since
	 * calling levels won't care.  Also, in a single-column index, we expect
	 * that PickSplit (or the special cases above) produced correct union
	 * datums.
	 */
	if (attno == 0 && giststate->nonLeafTupdesc->natts > 1)
	{
		v->spl_dontcare = NULL;
```
## Detailed Description
This function implements the sophisticated splitting algorithm for GiST index pages, handling multi-column indexes with recursive optimization. The process involves several phases:

1. **Entry Vector Preparation**: Creates a vector of GISTENTRY structures from the input tuples and identifies tuples with null values in the current column.

2. **Null Handling**: Implements a policy of separating null and non-null values, placing nulls on the right side and non-nulls on the left side to avoid mixing them on the same page.

3. **User-Defined Splitting**: For non-null values, invokes gistUserPicksplit to apply the opclass-specific splitting method with don't-care tuple optimization.

4. **Recursive Processing**: When the current column split is suboptimal, recursively processes subsequent columns to optimize don't-care tuple placement or handle degenerate splits.

5. **Union Key Computation**: Ensures correct union keys are computed for all columns, especially important for multi-column indexes and after recursive splitting.

The function handles edge cases like all-null columns, mixed null/non-null scenarios, and optimizes splits through intelligent tuple redistribution based on multiple column criteria.

## Parameters / Member Variables
- : The index relation being split
- : The page being split (used for entry initialization)
- : Array of IndexTuples to be processed (must contain at least 2 tuples)
- : Number of IndexTuples in the array
- : GiST state containing operator class methods and tuple descriptors
- : Working state and output area containing split vectors and union keys
- : Current column being processed (zero-based, initially 0 from external caller)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [index_getattr](../i/index_getattr.md) (extract column values from tuples)
  - gistdentryinit (initialize GIST entries)
  - [gistSplitHalf](gistSplitHalf.md) (fallback even split)
  - [gistUserPicksplit](gistUserPicksplit.md) (user-defined splitting with optimization)
  - [gistunionsubkey](gistunionsubkey.md) (union key computation)
  - memcpy (memory copying for backup operations)
- Types referenced:
  - [Relation](../R/Relation.md), Page, IndexTuple
  - [GISTSTATE](../G/GISTSTATE.md), GistSplitVector, GIST_SPLITVEC
  - [GistEntryVector](../G/GistEntryVector.md), GISTENTRY
  - OffsetNumber, Datum
- Constants used:
  - GEVHDRSZ (GistEntryVector header size)
- Called from:
  - [gistSplit](gistSplit.md) (main external caller)
  - [gistSplitByKey](gistSplitByKey.md) (recursive self-calls)

## Notes and Other Information
- The function is designed to be called initially with attno=0, with internal recursion incrementing attno
- Handles the complex case of don't-care tuples that can be optimally placed using subsequent columns
- Implements sophisticated backup and restoration of split vectors during recursive processing
- Ensures union keys are correctly computed at the top level (attno=0) for multi-column indexes  
- The caller must initialize spl_lisnull and spl_risnull arrays to all-true before calling
- Uses a mapping system to track tuple positions during recursive splitting operations
- Designed to handle any number of index columns through recursive processing