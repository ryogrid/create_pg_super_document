# pathkeys_useful_for_setop

## Location
[src/backend/optimizer/path/pathkeys.c:2197-2211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L2197-L2211)

## Overview
Counts the number of leading common pathkeys between the query's setop pathkeys and a given pathkeys list for set operations optimization.

## Definition

```c
structively, but we can avoid
	 * copying the list if we're not actually going to change it
	 */
	if (nuseful == 0)
		return NIL;
```
## Detailed Description
This function evaluates how many pathkeys from a given list are useful for set operations (such as UNION, INTERSECT, EXCEPT) by counting the number of leading common pathkeys between the query's  and the provided pathkeys list.

The function uses  to determine the overlap between  and the input pathkeys. This count is crucial for optimizing set operations, as having pre-sorted data that matches the setop requirements can significantly improve performance by avoiding or reducing the need for explicit sorting during set operation processing.

Set operations typically require data to be sorted in a specific order to efficiently perform operations like duplicate elimination, union, intersection, or difference computations. By identifying how many leading pathkeys align with the setop requirements, the planner can make informed decisions about whether to use existing ordering or invest in additional sorting.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context including setop pathkeys
- : List of PathKey structures to evaluate for set operation usefulness

## Dependencies
- Functions called/Symbols referenced:
  - [pathkeys_count_contained_in](pathkeys_count_contained_in.md) (function to count common pathkeys)
- Called from (representative examples):
  - [truncate_useless_pathkeys](../t/truncate_useless_pathkeys.md)

## Notes and Other Information
- This function is specifically designed for optimizing set operations (UNION, INTERSECT, EXCEPT)
- Returns the count of useful leading pathkeys that match setop requirements
- The function is static and used internally within the pathkeys.c module
- Set operations can benefit significantly from pre-sorted data, making this optimization important for query performance