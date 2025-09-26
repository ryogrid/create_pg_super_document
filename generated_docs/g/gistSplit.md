# gistSplit

## Location
[src/backend/access/gist/gist.c:1445-1531](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gist.c#L1445-L1531)

## Overview
The recursive core function that splits a GiST page into multiple pages when tuples cannot fit, utilizing the GiST splitting algorithm to determine optimal partitioning.

## Definition

```c
SplitPageLayout *
gistSplit(Relation r,
		  Page page,
		  IndexTuple *itup,		/* contains compressed entry */
		  int len,
		  GISTSTATE *giststate)
```
## Detailed Description
gistSplit is the fundamental page-splitting function in the GiST access method that recursively divides pages until all resulting pages can accommodate their assigned tuples. The function implements a sophisticated splitting strategy that uses the GiST-specific gistSplitByKey algorithm to determine optimal tuple distribution across multiple pages.

The splitting process begins by calling gistSplitByKey to partition tuples into left and right groups based on index key attributes. After partitioning, the function validates that each group fits within page size constraints using gistfitpage. If either group still exceeds page capacity, the function recursively calls itself to further subdivide the oversized groups.

The function constructs a linked list of SplitPageLayout structures representing the final page layout, with each node containing tuple lists, counts, and representative tuples (downlinks) for parent page updates. This recursive approach ensures that complex splitting scenarios involving multiple attributes and large tuple sets are handled correctly.

## Parameters / Member Variables
- : Relation pointer for the GiST index being modified
- : Page being split (used for context in splitting decisions)
- : Array of IndexTuple pointers containing all tuples to be distributed across split pages
- : Number of tuples in the itup array (must be > 1 for meaningful splits)
- : GISTSTATE containing cached access method information and tuple descriptor details

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md) (stack overflow protection for deep recursion)
  - [gistSplitByKey](gistSplitByKey.md) (core GiST splitting algorithm implementation)
  - [gistfitpage](gistfitpage.md) (page capacity validation)
  - [gistfillitupvec](gistfillitupvec.md) (tuple vector construction for page layouts)
  - [gistFormTuple](gistFormTuple.md) (downlink tuple creation for parent updates)
  - IndexTupleSize (tuple size calculation)
  - [GistSplitVector](../G/GistSplitVector.md) (split decision data structure)
  - [SplitPageLayout](../S/SplitPageLayout.md) (split result representation)
- Called from (representative examples):
  - [gistplacetopage](gistplacetopage.md) (main page insertion logic)
  - [gistSplit](gistSplit.md) (recursive self-calls for further subdivision)
  - [gist_indexsortbuild_levelstate_flush](gist_indexsortbuild_levelstate_flush.md) (index build operations)

## Notes and Other Information
- Implements recursive splitting to handle cases where initial split still produces oversized pages
- Validates single tuple size constraints and raises errors for tuples exceeding page capacity
- Constructs linked list of SplitPageLayout nodes representing final multi-page split result
- Uses ROTATEDIST macro for memory management and layout structure initialization
- Critical for handling complex multi-dimensional index splitting scenarios
- Forms appropriate downlink tuples for each split page to maintain tree consistency
- Stack depth checking prevents infinite recursion in pathological cases
- Essential component of GiST's ability to handle variable-sized and complex data types