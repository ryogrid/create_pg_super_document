# SplitLR

## Location
[src/backend/utils/adt/rangetypes_gist.c:66-87](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_gist.c#L66-L87)

## Overview
An enumeration type that indicates whether a GiST index entry should be placed on the left or right side during a range split operation.

## Definition

```c
typedef struct
{
	TypeCacheEntry *typcache;	/* typcache for range type */
	bool		has_subtype_diff;	/* does it have subtype_diff? */
	int			entries_count;	/* total number of entries being split */

	/* Information about currently selected split follows */

	bool		first;			/* true if no split was selected yet */

	RangeBound *left_upper;		/* upper bound of left interval */
	RangeBound *right_lower;	/* lower bound of right interval */

	float4		ratio;			/* split ratio */
	float4		overlap;		/* overlap between left and right predicate */
	int			common_left;	/* # common entries destined for each side */
	int			common_right;
} ConsiderSplitContext;
```
## Detailed Description
SplitLR is a simple enumeration used in the PostgreSQL GiST (Generalized Search Tree) implementation for range types. It provides a clear, type-safe way to indicate the direction of a split operation when partitioning range entries during index construction or maintenance. The enum values are designed to make initialization to SPLIT_LEFT straightforward by assigning it the value 0.

## Parameters / Member Variables
- : Indicates that an entry should be placed in the left partition of a split (value: 0)
- : Indicates that an entry should be placed in the right partition of a split

## Dependencies
- Functions called/Symbols referenced:
  - (none - this is a simple enum)
- Called from (representative examples):
  - rangeCopy
  - [range_gist_picksplit](../r/range_gist_picksplit.md)
  - [range_gist_class_split](../r/range_gist_class_split.md)

## Notes and Other Information
- Defined in src/backend/utils/adt/rangetypes_gist.c:64-66
- The explicit assignment of 0 to SPLIT_LEFT makes it the default value when variables of this type are initialized
- Used throughout the range type GiST implementation to maintain clarity about split directions
- Part of PostgreSQL's range type indexing infrastructure