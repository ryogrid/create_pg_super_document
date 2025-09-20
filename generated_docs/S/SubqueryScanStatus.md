# SubqueryScanStatus

## Location
[src/include/nodes/plannodes.h:596-597](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L596-L597)

## Overview
An enumeration that caches the trivial_subqueryscan property of a SubqueryScan node, used during query planning to determine if a SubqueryScan node can be optimized away.

## Definition

```c
typedef struct SubqueryScan
{
	Scan		scan;
	Plan	   *subplan;
	SubqueryScanStatus scanstatus;
} SubqueryScan;
```
## Detailed Description
SubqueryScanStatus is used to cache the result of analyzing whether a SubqueryScan node is "trivial" and can potentially be eliminated from the query plan. This caching mechanism avoids redundant analysis during the planning phase. The status is determined by the  function, which checks if the SubqueryScan node simply passes through data from its subplan without any additional processing (no quals, same target list structure, etc.).

A SubqueryScan is considered trivial when it doesn't add any meaningful processing layer and can be safely removed from the plan tree, allowing the executor to directly access the subplan's results.

## Parameters / Member Variables
- `scan`: Initial state indicating the trivial status has not yet been determined during planning
- `*subplan`: The SubqueryScan node is trivial and can be eliminated from the plan tree
- `scanstatus`: The SubqueryScan node performs meaningful work and cannot be eliminated
## Dependencies
- Functions called/Symbols referenced: None (enum definition)
- Used by:
  - SubqueryScan struct (as scanstatus member)
  - [trivial_subqueryscan](../t/trivial_subqueryscan.md)() function in setrefs.c

## Notes and Other Information
- This enum is only used during planning phase, not during execution
- The caching mechanism improves planning performance by avoiding repeated analysis
- Initially set to SUBQUERY_SCAN_UNKNOWN when a SubqueryScan node is created
- The trivial_subqueryscan() function first marks the status as NONTRIVIAL, then changes it to TRIVIAL only if all conditions are met
- This optimization is part of PostgreSQL's plan tree simplification process