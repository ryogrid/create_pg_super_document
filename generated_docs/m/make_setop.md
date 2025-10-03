# make_setop

## Location
[src/backend/optimizer/plan/createplan.c:6884-6939](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L6884-L6939)

## Overview
Creates a SetOp plan node that implements set operations like UNION, INTERSECT, and EXCEPT by filtering duplicate tuples based on specified columns.

## Definition

```c
static SetOp *
make_setop(SetOpCmd cmd, SetOpStrategy strategy, Plan *lefttree,
		   List *distinctList, AttrNumber flagColIdx, int firstFlag,
		   long numGroups)
```
## Detailed Description
The  function constructs a SetOp plan node that performs set operations (UNION, INTERSECT, EXCEPT) on sorted input data. The node works by examining consecutive tuples in the sorted input stream and applying the specified set operation logic to eliminate duplicates or find intersections/differences. The function converts a list of SortGroupClause specifications into arrays of column indices, equality operators, and collations that the executor can use efficiently during runtime.

The SetOp node assumes its input is already sorted according to the distinctList specification. It processes tuples sequentially, comparing them using the provided equality operators and collations to determine whether to include, exclude, or mark tuples based on the set operation being performed.

## Parameters / Member Variables
- `cmd`: The type of set operation to perform (UNION, INTERSECT, EXCEPT)
- `strategy`: The execution strategy for the set operation (e.g., sorted vs hashed)
- `*lefttree`: The input plan node providing sorted tuples to process
- `*distinctList`: List of SortGroupClause objects identifying columns to compare for distinctness
- `flagColIdx`: Column index for a flag column used in some set operations
- `firstFlag`: Value of the flag for the first input relation
- `numGroups`: Estimated number of distinct groups in the result
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates a new SetOp node)
  - [list_length](../l/list_length.md) (gets the number of columns to compare)
  - [palloc](../p/palloc.md) (allocates memory for operator arrays)
  - [get_sortgroupclause_tle](../g/get_sortgroupclause_tle.md) (extracts target entry from sort clause)
  - [exprCollation](../e/exprCollation.md) (gets collation for expression comparison)
  - [SetOpCmd](../S/SetOpCmd.md), SetOpStrategy, SortGroupClause (related data types)
- Called from (representative examples):
  - [create_setop_plan](../c/create_setop_plan.md)
  - CP_IGNORE_TLIST

## Notes and Other Information
- The function is static and only used within createplan.c
- Input data must be pre-sorted according to distinctList for correct operation
- Converts SortGroupClause list into parallel arrays (dupColIdx, dupOperators, dupCollations) for efficient executor access
- The flagColIdx and firstFlag parameters are used to distinguish between different input relations in complex set operations
- Memory allocation for operator arrays uses palloc, which is PostgreSQL's memory management system
- The numGroups parameter helps the executor estimate memory usage and choose appropriate algorithms

## Simplified Source

```c
static SetOp *
make_setop(SetOpCmd cmd, SetOpStrategy strategy, Plan *lefttree,
           List *distinctList, AttrNumber flagColIdx, int firstFlag,
           long numGroups)
{
    SetOp *node = makeNode(SetOp);
    Plan *plan = &node->plan;

    // Set up basic plan structure
    plan->targetlist = lefttree->targetlist;
    plan->qual = NIL;
    plan->lefttree = lefttree;
    plan->righttree = NULL;

    // Convert SortGroupClause list to arrays for executor
    int numCols = list_length(distinctList);
    AttrNumber *dupColIdx = palloc(sizeof(AttrNumber) * numCols);
    Oid *dupOperators = palloc(sizeof(Oid) * numCols);
    Oid *dupCollations = palloc(sizeof(Oid) * numCols);

    // Extract column info from each sort clause
    int keyno = 0;
    foreach(cell, distinctList)
    {
        SortGroupClause *sortcl = lfirst(cell);
        TargetEntry *tle = get_sortgroupclause_tle(sortcl, plan->targetlist);

        dupColIdx[keyno] = tle->resno;
        dupOperators[keyno] = sortcl->eqop;
        dupCollations[keyno] = exprCollation(tle->expr);
        keyno++;
    }

    // Configure SetOp node parameters
    node->cmd = cmd;
    node->strategy = strategy;
    node->numCols = numCols;
    node->dupColIdx = dupColIdx;
    node->dupOperators = dupOperators;
    node->dupCollations = dupCollations;
    node->flagColIdx = flagColIdx;
    node->firstFlag = firstFlag;
    node->numGroups = numGroups;

    return node;
}
```