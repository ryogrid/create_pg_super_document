# get_result_relid

## Location
[src/backend/optimizer/prep/prepjointree.c:3771-3800](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L3771-L3800)

## Overview
A utility function that determines if a join tree node is a RangeTblRef pointing to an RTE_RESULT RTE and returns its relation ID if so.

## Definition
```c
static int get_result_relid(PlannerInfo *root, Node *jtnode)
```

## Detailed Description
This helper function provides a simple check to identify RTE_RESULT relations within the join tree. It examines a join tree node to determine if it represents a reference to a result relation (RTE_RESULT), which are special RTEs that return exactly one row with no output columns.

The function performs three checks:
1. Verifies the node is a RangeTblRef (not a complex join structure)
2. Extracts the rtindex from the RangeTblRef
3. Looks up the RTE in the range table to confirm it's of type RTE_RESULT

This is used extensively during RTE_RESULT optimization to identify candidates for removal or transformation.

## Parameters / Member Variables
- : PlannerInfo containing the query parse tree and range table
- : Join tree node to examine for RTE_RESULT status

## Dependencies
- Functions called/Symbols referenced:
  - rt_fetch (macro to access range table entries)
  - [RangeTblRef](../R/RangeTblRef.md) (type checking and casting)
  - RTE_RESULT (constant for relation type comparison)

- Called from (representative examples):
  - [remove_useless_results_recurse](../r/remove_useless_results_recurse.md) (5 different locations for various join optimization scenarios)

## Notes and Other Information
- This is a static function, only accessible within prepjointree.c
- Returns 0 (invalid relid) if the node is not a RTE_RESULT reference
- Returns the actual rtindex (relation ID) if it is a RTE_RESULT reference
- Simple utility function that encapsulates the common pattern of checking for RTE_RESULT RTEs
- Used as a guard condition before applying RTE_RESULT-specific optimizations
- Part of the join tree optimization infrastructure in PostgreSQL's query planner

## Simplified Source

```c
static int get_result_relid(PlannerInfo *root, Node *jtnode)
{
    int varno;

    // Check if this is a simple range table reference
    if (!IsA(jtnode, RangeTblRef))
        return 0;

    // Get the relation index
    varno = ((RangeTblRef *) jtnode)->rtindex;

    // Check if it's actually an RTE_RESULT relation
    if (rt_fetch(varno, root->parse->rtable)->rtekind != RTE_RESULT)
        return 0;

    return varno;
}
```