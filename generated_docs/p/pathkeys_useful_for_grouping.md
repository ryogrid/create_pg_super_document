# pathkeys_useful_for_grouping

## Location
[src/backend/optimizer/path/pathkeys.c:2167-2196](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L2167-L2196)

## Overview
Counts the number of pathkeys that are useful for grouping operations, allowing for potential reordering to benefit from existing ordering and incremental sort optimization.

## Definition

```c
static int
pathkeys_useful_for_grouping(PlannerInfo *root, List *pathkeys)
```
## Detailed Description
This function evaluates how many leading pathkeys from a given list can be leveraged for GROUP BY operations. The function implements a sophisticated optimization strategy where group pathkeys can be reordered to benefit from existing ordering, potentially allowing incremental sort instead of a full explicit sort operation.

The algorithm walks through the provided pathkeys sequentially and counts how many consecutive pathkeys from the beginning have matching entries in the query's . It stops at the first pathkey that doesn't have a corresponding group key. This prefix-based approach ensures that the optimizer can take advantage of partial ordering.

For example, with pathkeys (a,b,c) and group keys (a,b,e), the function determines that (a,b) pathkeys are useful for grouping, potentially enabling an incremental sort to achieve the final ordering of (a,b,e) rather than sorting from scratch.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context including group pathkeys
- : List of PathKey structures to evaluate for grouping usefulness

## Dependencies
- Functions called/Symbols referenced:
  - [PathKey](../P/PathKey.md) (structure type)
  - [list_member_ptr](../l/list_member_ptr.md) (function to check list membership)
- Called from (representative examples):
  - [truncate_useless_pathkeys](../t/truncate_useless_pathkeys.md)

## Notes and Other Information
- Returns 0 if no special ordering is requested for grouping (group_pathkeys is NIL)
- The function supports incremental sort optimization for partially ordered data
- Only counts consecutive matching pathkeys from the beginning of the list
- This optimization can significantly reduce sorting overhead when data is already partially ordered according to grouping requirements
- The logic preserves paths with ordering that doesn't directly match grouping keys but can still be beneficial with reordering