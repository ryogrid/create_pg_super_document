# subbuild_joinrel_restrictlist

## Location
[src/backend/optimizer/util/relnode.c:1352-1417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L1352-L1417)

## Overview
Processes joininfo clauses from an input relation to build the restriction clause list for a new join relation.

## Definition

```c
static List *
subbuild_joinrel_restrictlist(PlannerInfo *root,
							  RelOptInfo *joinrel,
							  RelOptInfo *input_rel,
							  Relids both_input_relids,
							  List *new_restrictlist)
```
## Detailed Description
The  function examines each joininfo clause from an input relation and determines whether it should become a restriction clause for the new join relation. A clause becomes a restriction clause if it refers only to relations within the joinrel (i.e., no outside relations).

The function handles special logic for clone clauses, which are created during outer join processing. For clone clauses, it must verify that the clause can be safely evaluated at this join level by checking required_relids and incompatible_relids. For non-clone clauses, it asserts that the clause is properly positioned.

The function carefully eliminates duplicates using pointer equality comparison, since RestrictInfo nodes are multiply-linked rather than copied across different joinlists.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing global query planner state
- `*joinrel`: The new join relation being constructed
- `*input_rel`: The input relation whose joininfo clauses are being processed
- `both_input_relids`: Relids representing both inputs to the join (used for clone clause validation)
- `*new_restrictlist`: Existing restriction list to which new clauses will be appended
## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_subset](../b/bms_is_subset.md)
  - RINFO_IS_PUSHED_DOWN
  - [bms_overlap](../b/bms_overlap.md)
  - [list_append_unique_ptr](../l/list_append_unique_ptr.md)
- Called from (representative examples):
  - build_joinrel_restrictlist

## Notes and Other Information
- This is a static function within relnode.c, used internally for join relation construction
- The function is part of the query optimizer's join processing logic
- Clone clauses require special handling due to outer join semantics and timing constraints
- Duplicate elimination is crucial since the same RestrictInfo nodes may appear in multiple joininfo lists
- The function operates at lines 1352-1417 in src/backend/optimizer/util/relnode.c
- Clauses that still reference outside relations remain as join clauses and are ignored by this function

## Simplified Source

```c
static List *
subbuild_joinrel_restrictlist(PlannerInfo *root,
                              RelOptInfo *joinrel,
                              RelOptInfo *input_rel,
                              Relids both_input_relids,
                              List *new_restrictlist)
{
    ListCell *l;

    // Examine each joininfo clause from the input relation
    foreach(l, input_rel->joininfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

        // Check if clause should become a restriction clause for this joinrel
        if (bms_is_subset(rinfo->required_relids, joinrel->relids))
        {
            // Handle special clone clause validation
            if (rinfo->has_clone || rinfo->is_clone)
            {
                // Verify clone clause can be safely evaluated at this join level
                if (!bms_is_subset(rinfo->required_relids, both_input_relids))
                    continue;
                if (bms_overlap(rinfo->incompatible_relids, both_input_relids))
                    continue;
            }

            // Add clause to restriction list, avoiding duplicates
            new_restrictlist = list_append_unique_ptr(new_restrictlist, rinfo);
        }
        // Clauses that reference outside relations remain as join clauses (ignored)
    }

    return new_restrictlist;
}
```