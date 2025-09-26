# make_sort_from_sortclauses

## Location
[src/backend/optimizer/plan/createplan.c:6416-6464](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L6416-L6464)

## Overview
Creates a Sort plan node to sort input tuples according to given sort clauses, converting a list-based representation into the array format required by the executor.

## Definition

```c
Sort *
make_sort_from_sortclauses(List *sortcls, Plan *lefttree)
```
## Detailed Description
This function constructs a Sort plan node from a list of SortGroupClauses. It performs the crucial task of transforming the list-based sort specification used during planning into the array-based format that the PostgreSQL executor expects. The function extracts sorting information including column indices, sort operators, collations, and null handling preferences from each SortGroupClause, then delegates to make_sort() to create the actual Sort node.

The function allocates arrays to hold the sorting specification and iterates through the provided sort clauses, extracting the target entry for each sort column and gathering the necessary sorting metadata.

## Parameters / Member Variables
- : A List of SortGroupClause structures specifying the sort criteria
- : The input Plan node that provides the tuples to be sorted

## Dependencies
- Functions called/Symbols referenced:
  - [SortGroupClause](../S/SortGroupClause.md) (struct type)
  - [get_sortgroupclause_tle](../g/get_sortgroupclause_tle.md)
  - [exprCollation](../e/exprCollation.md)
  - [make_sort](make_sort.md)
  - [Sort](../S/Sort.md) (return type)
- Called from (representative examples):
  - [create_unique_plan](../c/create_unique_plan.md)

## Notes and Other Information
- The function performs memory allocation for four arrays: sortColIdx, sortOperators, collations, and nullsFirst
- It uses palloc() for memory allocation, which is PostgreSQL's memory management system
- The conversion from list to arrays is necessary because the executor expects array-based sort specifications for performance reasons
- Located in src/backend/optimizer/plan/createplan.c at lines 6416-6464