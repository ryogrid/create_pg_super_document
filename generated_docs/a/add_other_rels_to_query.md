# add_other_rels_to_query

## Location
[src/backend/optimizer/plan/initsplan.c:195-233](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L195-L233)

## Overview
Creates "otherrel" RelOptInfos for the children of appendrel baserels, completing the relation structure setup for query planning.

## Definition
```c
void add_other_rels_to_query(PlannerInfo *root)
```

## Detailed Description
This function processes all base relations that were previously created by add_base_rels_to_query and expands any that are marked as inheritable (appendrels). It iterates through the simple_rel_array to find base relations that have inheritance hierarchies and creates additional RelOptInfo structures for their child relations.

The function works by:
1. Scanning through all entries in the simple_rel_array
2. Skipping empty slots and non-baserel entries
3. Checking if each base relation is marked as inheritable (rte->inh is true)
4. For inheritable relations, calling expand_inherited_rtentry() to create child relation structures

This is a crucial step in query planning for partitioned tables and inheritance hierarchies, as it ensures that all relations that will be scanned during query execution have corresponding RelOptInfo structures.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the query planning context, including the simple_rel_array and simple_rte_array

## Dependencies
- Functions called/Symbols referenced:
  - [expand_inherited_rtentry](../e/expand_inherited_rtentry.md)
  - RELOPT_BASEREL (constant)
- Data structures used:
  - [RelOptInfo](../R/RelOptInfo.md)
  - [RangeTblEntry](../R/RangeTblEntry.md)
- Called from (representative examples):
  - [query_planner](../q/query_planner.md)

## Notes and Other Information
- This function is called after add_base_rels_to_query() has completed
- It specifically handles inheritance and partitioning scenarios
- Empty slots in simple_rel_array correspond to non-baserel RTEs and are safely skipped
- Only processes RELOPT_BASEREL relations, ignoring any "otherrels" that were already added
- After completion, there should be RelOptInfos for all relations that will be scanned by the query
- Located in src/backend/optimizer/plan/initsplan.c at lines 195-233

## Simplified Source

```c
void add_other_rels_to_query(PlannerInfo *root)
{
    int rti;

    // Scan through all relations in the simple_rel_array
    for (rti = 1; rti < root->simple_rel_array_size; rti++)
    {
        RelOptInfo *rel = root->simple_rel_array[rti];
        RangeTblEntry *rte = root->simple_rte_array[rti];

        // Skip empty slots (non-baserel RTEs)
        if (rel == NULL)
            continue;

        // Skip relations that are not base relations
        if (rel->reloptkind != RELOPT_BASEREL)
            continue;

        // If this relation has inheritance, expand its children
        if (rte->inh)
            expand_inherited_rtentry(root, rel, rte, rti);
    }
}
```