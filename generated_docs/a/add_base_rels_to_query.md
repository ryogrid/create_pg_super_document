# add_base_rels_to_query

## Location
src/backend/optimizer/plan/initsplan.c: 157 - 194

## Overview
Scans the query's jointree and creates baserel RelOptInfos for all the base relations (tables, subqueries, and function RTEs) appearing in the jointree.

## Definition
```c
void add_base_rels_to_query(PlannerInfo *root, Node *jtnode)
```

## Detailed Description
This function is a recursive tree traversal routine that processes the query's join tree structure to identify and create RelOptInfo structures for base relations. It handles three main types of join tree nodes:

1. **RangeTblRef**: Direct references to base relations - creates a simple relation using build_simple_rel()
2. **FromExpr**: FROM clause expressions containing a list of relations - recursively processes each item in the fromlist
3. **JoinExpr**: JOIN expressions - recursively processes both left and right arguments

The function is fundamental to query planning as it establishes the basic relation structures that will later be used for join planning and optimization. It only handles base relations; appendrel parents may require additional "otherrel" RelOptInfos that are added in later processing phases.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context and state
- `jtnode`: Node representing a portion of the query's join tree to process (initially root->parse->jointree)

## Dependencies
- Functions called/Symbols referenced:
  - build_simple_rel
  - nodeTag
  - elog (for error handling)
- Data structures used:
  - RangeTblRef
  - FromExpr
  - JoinExpr
- Called from (representative examples):
  - query_planner (main entry point)
  - Self-recursion for tree traversal

## Notes and Other Information
- The initial invocation must pass root->parse->jointree as the jtnode parameter
- This is a recursive function that traverses the entire join tree structure
- Creates one baserel RelOptInfo for every non-join RTE used in the query
- Appendrel members are handled separately in later processing phases
- Error handling includes checking for unrecognized node types
- Located in src/backend/optimizer/plan/initsplan.c at lines 157-194