# make_nestloop

## Location
[src/backend/optimizer/plan/createplan.c:5949-5973](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L5949-L5973)

## Overview
Creates a NestLoop plan node that represents a nested loop join operation in PostgreSQL's query execution plan tree.

## Definition
```c
static NestLoop *make_nestloop(List *tlist, List *joinclauses, List *otherclauses, List *nestParams, Plan *lefttree, Plan *righttree, JoinType jointype, bool inner_unique)
```

## Detailed Description
The `make_nestloop` function constructs a NestLoop plan node, which implements the nested loop join algorithm. This is one of the fundamental join algorithms in PostgreSQL where for each row in the outer relation (left tree), the inner relation (right tree) is scanned to find matching rows based on the join conditions. The function initializes all necessary fields of the NestLoop structure, including join-specific parameters and nested loop parameters that enable parameterized scans of the inner relation.

This join method is typically chosen when one of the relations is small, when there's a suitable index on the inner relation for the join condition, or when other join methods are not applicable.

## Parameters / Member Variables
- `tlist`: Target list specifying the columns to be output by this join node
- `joinclauses`: List of join qualification clauses that determine matching conditions between outer and inner relations
- `otherclauses`: List of other qualification clauses (non-join conditions) to be applied at this node
- `nestParams`: List of parameters that will be passed from outer to inner relation for parameterized nested loop execution
- `lefttree`: Plan node representing the outer (driving) relation
- `righttree`: Plan node representing the inner relation to be scanned for each outer row
- `jointype`: Type of join operation (INNER, LEFT, RIGHT, FULL, etc.)
- `inner_unique`: Boolean indicating whether the inner relation produces at most one matching row for each outer row

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create the NestLoop node)
  - [NestLoop](../N/NestLoop.md) (plan node structure)
  - JoinType (enumeration for join types)
- Called from (representative examples):
  - [create_nestloop_plan](../c/create_nestloop_plan.md) (in createplan.c:4425)

## Notes and Other Information
- This is a static function within createplan.c, used internally by the plan creation subsystem
- The nested loop join is the most basic join algorithm, with O(M*N) complexity in the worst case
- The `nestParams` field enables efficient parameterized nested loops where inner scans can use parameters from the current outer row
- The `inner_unique` flag can enable optimizations during execution when the planner knows the inner relation will produce unique matches
- [NestLoop](../N/NestLoop.md) joins are often chosen when suitable indexes exist on the inner relation's join columns

## Simplified Source

```c
static NestLoop *
make_nestloop(List *tlist,
              List *joinclauses,
              List *otherclauses,
              List *nestParams,
              Plan *lefttree,
              Plan *righttree,
              JoinType jointype,
              bool inner_unique)
{
    // Create a new NestLoop plan node
    NestLoop *node = makeNode(NestLoop);
    Plan *plan = &node->join.plan;

    // Set basic plan properties
    plan->targetlist = tlist;
    plan->qual = otherclauses;
    plan->lefttree = lefttree;
    plan->righttree = righttree;

    // Set join properties
    node->join.jointype = jointype;
    node->join.inner_unique = inner_unique;
    node->join.joinqual = joinclauses;

    // Set nested loop specific parameters
    node->nestParams = nestParams;

    return node;
}
```