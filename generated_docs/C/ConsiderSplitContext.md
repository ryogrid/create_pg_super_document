# ConsiderSplitContext

## Location
[src/backend/access/gist/gistproc.c:300-308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistproc.c#L300-L308)

## Overview
ConsiderSplitContext is a structure used in PostgreSQL's GiST (Generalized Search Tree) index implementation to maintain context information during the process of selecting optimal page splitting strategies for geometric data types like boxes.

## Definition

```c
typedef struct
{
	float8		lower,
				upper;
} SplitInterval;
```
## Detailed Description
ConsiderSplitContext serves as a context structure for the  function, which is part of PostgreSQL's optimized GiST index page splitting algorithm for geometric data types. This structure maintains information about the current best split candidate during the double sorting-based node splitting process described in "A new double sorting-based node splitting algorithm for R-tree" by A. Korotkov.

The structure tracks both general information about all entries being split (total count and overall bounding box) and specific details about the currently selected split candidate (bounds, ratios, overlap metrics, and dimensional information). This allows the splitting algorithm to efficiently evaluate multiple split options and select the one that minimizes overlap and provides balanced distribution of entries.

## Parameters / Member Variables
- `entriesCount`: Total number of entries that need to be split across the two resulting pages
- `boundingBox`: The minimum bounding rectangle (MBR) that encompasses all entries being split
- `first`: Boolean flag indicating whether this is the first split being considered (no previous split selected)
- `leftUpper`: Upper bound of the interval for entries that would go to the left page in the current split candidate
- `rightLower`: Lower bound of the interval for entries that would go to the right page in the current split candidate  
- `ratio`: Distribution ratio metric for the current split candidate
- `overlap`: Overlap metric measuring how much the left and right intervals overlap in the current split
- `dim`: The dimensional axis (0 for x-axis, 1 for y-axis) along which the current split is being considered
- `range`: The width of the overall MBR when projected onto the selected dimensional axis

## Dependencies
- Functions called/Symbols referenced:
  - [BOX](../B/BOX.md) (geometric data type)
  - float8, float4 (PostgreSQL numeric types)
  - int, bool (standard types)
- Called from (representative examples):
  - [g_box_consider_split](../g/g_box_consider_split.md)
  - [gist_box_picksplit](../g/gist_box_picksplit.md)
  - [range_gist_consider_split](../r/range_gist_consider_split.md)
  - [range_gist_double_sorting_split](../r/range_gist_double_sorting_split.md)

## Notes and Other Information
This structure is central to PostgreSQL's implementation of an advanced R-tree node splitting algorithm that aims to minimize overlap between sibling pages while maintaining balanced distribution of entries. The algorithm considers splits along both X and Y axes and uses double sorting to efficiently evaluate split quality. The context structure allows the algorithm to maintain state across multiple split evaluations and select the optimal split based on combined metrics of overlap, ratio, and spatial distribution. This is particularly important for maintaining good query performance in GiST indexes used for geometric and range data types.