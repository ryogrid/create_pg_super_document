# make_group

## Location
[src/backend/optimizer/plan/createplan.c:6670-6699](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L6670-L6699)

## Overview
Creates and initializes a Group plan node for eliminating consecutive duplicate groups in sorted input data.

## Definition
```c
static Group *make_group(List *tlist,
                        List *qual,
                        int numGroupCols,
                        AttrNumber *grpColIdx,
                        Oid *grpOperators,
                        Oid *grpCollations,
                        Plan *lefttree)
```

## Detailed Description
This static function constructs a Group plan node that performs grouping operations on pre-sorted input data. The Group node is designed to eliminate consecutive duplicate rows based on specified grouping columns, which is more efficient than hash-based grouping when the input is already sorted in the correct order. This node is commonly used in query plans where the optimizer determines that sort-based grouping is more efficient than hash-based grouping, particularly when the input is already sorted or when memory constraints favor streaming operations over building hash tables.

## Parameters / Member Variables
- `tlist`: Target list defining the output columns of the grouping operation
- `qual`: Qualification conditions (typically HAVING clauses) to be applied to groups
- `numGroupCols`: Number of columns used for grouping comparison
- `grpColIdx`: Array of attribute numbers identifying the grouping columns
- `grpOperators`: Array of operator OIDs for comparing grouping column values
- `grpCollations`: Array of collation OIDs for grouping columns (for text comparisons)
- `lefttree`: Left child plan node providing sorted input tuples

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create Group node)
- Types referenced:
  - [Group](../G/Group.md) (the grouping plan node structure)
- Called from (representative examples):
  - [create_group_plan](../c/create_group_plan.md)

## Notes and Other Information
- This is a static function, only accessible within the createplan.c file
- Requires input to be pre-sorted on the grouping columns for correct operation
- More memory-efficient than hash-based aggregation for large datasets when input is sorted
- The right child plan node is always set to NULL as grouping is a unary operation
- Often used in conjunction with Sort nodes to ensure proper input ordering
- [Group](../G/Group.md) nodes perform streaming aggregation, processing one group at a time without building large intermediate data structures
- The grouping columns must match exactly between consecutive rows to be considered part of the same group

## Simplified Source

```c
static Group *make_group(List *tlist, List *qual, int numGroupCols,
                        AttrNumber *grpColIdx, Oid *grpOperators,
                        Oid *grpCollations, Plan *lefttree) {
    // Create new Group node
    Group *node = makeNode(Group);
    Plan *plan = &node->plan;

    // Set grouping configuration
    node->numCols = numGroupCols;
    node->grpColIdx = grpColIdx;
    node->grpOperators = grpOperators;
    node->grpCollations = grpCollations;

    // Configure plan node
    plan->qual = qual;
    plan->targetlist = tlist;
    plan->lefttree = lefttree;
    plan->righttree = NULL;  // Group is unary operation

    return node;
}
```