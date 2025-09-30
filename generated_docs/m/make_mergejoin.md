# make_mergejoin

## Location
[src/backend/optimizer/plan/createplan.c:6028-6068](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L6028-L6068)

## Overview
Creates a MergeJoin plan node that represents a merge join operation in PostgreSQL's query execution plan tree.

## Definition
```c
static MergeJoin *make_mergejoin(List *tlist, List *joinclauses, List *otherclauses, List *mergeclauses, Oid *mergefamilies, Oid *mergecollations, int *mergestrategies, bool *mergenullsfirst, Plan *lefttree, Plan *righttree, JoinType jointype, bool inner_unique, bool skip_mark_restore)
```

## Detailed Description
The `make_mergejoin` function constructs a MergeJoin plan node, which implements the merge join algorithm. Merge joins work by taking two pre-sorted input relations and merging them together, similar to the merge step in merge sort. This join method is particularly efficient when both input relations are already sorted on the join keys or can be efficiently sorted.

The function initializes extensive merge-specific metadata including merge families, collations, strategies, and null handling rules that govern how the merge process operates. Merge joins can be very efficient with O(M+N) complexity when inputs are pre-sorted, but require both inputs to be ordered on the join keys.

## Parameters / Member Variables
- `tlist`: Target list specifying the columns to be output by this join node
- `joinclauses`: List of join qualification clauses that determine matching conditions between relations
- `otherclauses`: List of other qualification clauses (non-join conditions) to be applied at this node
- `mergeclauses`: List of clauses that will be used for the merge operation (must be sortable)
- `mergefamilies`: Array of operator family OIDs for each merge clause, defining the sort/comparison semantics
- `mergecollations`: Array of collation OIDs for each merge clause (important for string comparisons)
- `mergestrategies`: Array of strategy numbers indicating the comparison operator types (less than, greater than, etc.)
- `mergenullsfirst`: Array of booleans indicating whether NULL values should sort first for each merge key
- `lefttree`: Plan node representing the left (outer) relation
- `righttree`: Plan node representing the right (inner) relation
- `jointype`: Type of join operation (INNER, LEFT, RIGHT, FULL, etc.)
- `inner_unique`: Boolean indicating whether the inner relation produces at most one matching row for each outer row
- `skip_mark_restore`: Boolean optimization flag indicating whether mark/restore operations can be skipped

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create the MergeJoin node)
  - [MergeJoin](../M/MergeJoin.md) (plan node structure)
  - JoinType (enumeration for join types)
- Called from (representative examples):
  - [create_mergejoin_plan](../c/create_mergejoin_plan.md) (in createplan.c:4726)

## Notes and Other Information
- This is a static function within createplan.c, used internally by the plan creation subsystem
- Merge joins require both input relations to be sorted on the join keys, which may necessitate explicit Sort nodes in the plan tree
- The merge-specific arrays (mergefamilies, mergecollations, etc.) must have the same length as the mergeclauses list
- The `skip_mark_restore` optimization can be applied when the inner relation is known to be unique, eliminating the need for backtracking
- Merge joins are often chosen when both relations are large and already sorted, or when the cost of sorting is justified by the join efficiency
- The algorithm maintains position markers in both input streams and can handle duplicate values by using mark/restore operations to backtrack when necessary
- NULL handling is critical in merge joins and is controlled by the mergeNullsFirst array to ensure consistent ordering

## Simplified Source

```c
static MergeJoin *
make_mergejoin(List *tlist,
               List *joinclauses,
               List *otherclauses,
               List *mergeclauses,
               Oid *mergefamilies,
               Oid *mergecollations,
               int *mergestrategies,
               bool *mergenullsfirst,
               Plan *lefttree,
               Plan *righttree,
               JoinType jointype,
               bool inner_unique,
               bool skip_mark_restore)
{
    // Create a new MergeJoin plan node
    MergeJoin *node = makeNode(MergeJoin);
    Plan *plan = &node->join.plan;

    // Set basic plan properties
    plan->targetlist = tlist;
    plan->qual = otherclauses;
    plan->lefttree = lefttree;
    plan->righttree = righttree;

    // Set merge-specific properties
    node->mergeclauses = mergeclauses;
    node->mergeFamilies = mergefamilies;
    node->mergeCollations = mergecollations;
    node->mergeStrategies = mergestrategies;
    node->mergeNullsFirst = mergenullsfirst;

    // Set join properties
    node->join.jointype = jointype;
    node->join.inner_unique = inner_unique;
    node->join.joinqual = joinclauses;

    // Set optimization flag
    node->skip_mark_restore = skip_mark_restore;

    return node;
}
```