# make_rels_by_clause_joins

## Location
[src/backend/optimizer/path/joinrels.c:280-313](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L280-L313)

## Overview
Builds join relations between a given relation and other relations that participate in join clauses or join-order restrictions with it, used in PostgreSQL's join search algorithm.

## Definition

```c
static void
make_rels_by_clause_joins(PlannerInfo *root,
						  RelOptInfo *old_rel,
						  List *other_rels,
						  int first_rel_idx)
```
## Detailed Description
The  function systematically creates join relations between a specified relation () and a list of candidate relations (). It only creates joins when there are relevant join clauses or join-order restrictions between the relations. This function is part of PostgreSQL's dynamic programming approach to join optimization, ensuring that only meaningful joins are considered rather than generating all possible Cartesian products.

The function leverages the join_rel_level mechanism to automatically ensure that each new join relation is only added to the result list once, even when the same joined relation could be generated through multiple join sequences (e.g., (a join b) join c vs (b join c) join a).

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing the query planning context
- `*old_rel`: The relation entry for the relation to be joined with others
- `*other_rels`: A list containing the other relations to be considered for joining
- `first_rel_idx`: The index of the first relation to consider in 'other_rels' (allows partial iteration)
## Dependencies
- Functions called/Symbols referenced:
  - for_each_from
  - [bms_overlap](../b/bms_overlap.md)
  - [have_relevant_joinclause](../h/have_relevant_joinclause.md)
  - [have_join_order_restriction](../h/have_join_order_restriction.md)
  - [make_join_rel](make_join_rel.md)
- Called from (representative examples):
  - [join_search_one_level](../j/join_search_one_level.md)

## Notes and Other Information
- Currently used primarily with initial relations in other_rels, but the design supports joining to join relations as well
- The function checks for relation overlap using bitmapsets to ensure relations don't contain common base relations
- Results are automatically added to  by the underlying  function
- The same joined relation may be generated multiple ways at higher levels, but each contributes different sets of paths
- Static function scope limits its direct usage to within the same source file

## Simplified Source

```c
static void
make_rels_by_clause_joins(PlannerInfo *root,
                          RelOptInfo *old_rel,
                          List *other_rels,
                          int first_rel_idx)
{
    ListCell *l;

    // Iterate through candidate relations starting from specified index
    for_each_from(l, other_rels, first_rel_idx)
    {
        RelOptInfo *other_rel = (RelOptInfo *) lfirst(l);

        // Check if relations don't overlap (no common base relations)
        if (!bms_overlap(old_rel->relids, other_rel->relids) &&
            // Check if there's a reason to join these relations
            (have_relevant_joinclause(root, old_rel, other_rel) ||
             have_join_order_restriction(root, old_rel, other_rel)))
        {
            // Create the join relation
            (void) make_join_rel(root, old_rel, other_rel);
        }
    }
}
```

This function systematically evaluates potential join relationships by:
1. **Iterating through candidates**: Starting from a specified index in the relations list
2. **Checking compatibility**: Ensuring relations don't share base relations (preventing invalid joins)
3. **Validating join necessity**: Only creating joins where there are join clauses or ordering constraints
4. **Creating valid joins**: Delegating actual join creation to `make_join_rel()`

The simplified version preserves the core algorithm while adding clarifying comments for each logical step.