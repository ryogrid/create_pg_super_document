# make_pathtarget_from_tlist

## Location
[src/backend/optimizer/util/tlist.c:591-623](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L591-L623)

## Overview
Constructs a PathTarget data structure from a given targetlist, serving as a lightweight representation that strips away TargetEntry decorations while preserving essential expression and sorting information.

## Definition

```c
PathTarget *
make_pathtarget_from_tlist(List *tlist)
```
## Detailed Description
This function creates a PathTarget structure by extracting expressions and sort group references from a provided targetlist. The PathTarget is a streamlined version of a full targetlist that omits most TargetEntry decorations except for sortgroupref data, while adding placeholders for evaluation cost and output data width information. The function initializes the cost and width fields as zeroes, expecting callers to use create_pathtarget() if they need these fields properly computed.

The function iterates through each TargetEntry in the input list, extracting the expression (tle->expr) and sort group reference (tle->ressortgroupref), and stores them in the corresponding arrays within the PathTarget structure. The volatility status is initially marked as unknown and will be determined later when contain_volatile_functions is called.

## Parameters / Member Variables
- `*tlist`: A List of TargetEntry nodes representing the targetlist to be converted into a PathTarget
## Dependencies
- Functions called/Symbols referenced:
  - [PathTarget](../P/PathTarget.md) (data structure)
  - makeNode (node creation)
  - [palloc](../p/palloc.md) (memory allocation)
  - [list_length](../l/list_length.md) (list utility)
  - [lappend](../l/lappend.md) (list append)
  - VOLATILITY_UNKNOWN (volatility constant)
- Called from (representative examples):
  - create_pathtarget
  - Various optimizer functions that need PathTarget representations

## Notes and Other Information
- The resulting PathTarget has cost and width fields set to zero; most callers should use create_pathtarget() instead to get properly computed values
- The volatility is marked as unknown initially and will be computed on-demand when needed
- This is part of the PathTarget manipulation functions in the PostgreSQL optimizer
- The function is declared in src/include/optimizer/tlist.h