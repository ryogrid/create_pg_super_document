# check_index_predicates

## Location
[src/backend/optimizer/path/indxpath.c:3244-3381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L3244-L3381)

## Overview
Sets the predicate-derived IndexOptInfo fields for each index of a specified relation to determine partial index usability and compute restriction info.

## Definition

```c
struct a list of clauses that we can assume true for the purpose of
	 * proving the index(es) usable.  Restriction clauses for the rel are
	 * always usable, and so are any join clauses that are "movable to" this
	 * rel.  Also, we can consider any EC-derivable join clauses (which must
	 * be "movable to" this rel, by definition).
	 */
	clauselist = list_copy(rel->baserestrictinfo);
```
## Detailed Description
This function is a crucial part of PostgreSQL's query optimizer that handles partial index predicate analysis. It determines whether partial indexes can be used by checking if the query's WHERE clauses imply the index predicates. For each index, it sets the  field to true if the predicate is satisfied and computes  - the list of restriction conditions that remain after accounting for what the index predicate already guarantees.

The function constructs a comprehensive list of available clauses including restriction clauses, movable join clauses, and equivalence-derivable join clauses. Special handling is provided for target relations (UPDATE/DELETE/MERGE/SELECT FOR UPDATE) where implied quals cannot be removed due to EvalPlanQual requirements.

## Parameters / Member Variables
- : PlannerInfo structure containing global information about the query
- : RelOptInfo structure representing the relation whose indexes are being analyzed

## Dependencies
- Functions called/Symbols referenced:
  - IS_SIMPLE_REL
  - [list_copy](../l/list_copy.md)
  - [join_clause_is_movable_to](../j/join_clause_is_movable_to.md)
  - [bms_difference](../b/bms_difference.md)
  - [find_childrel_parents](../f/find_childrel_parents.md)
  - [bms_del_members](../b/bms_del_members.md)
  - bms_is_empty
  - [list_concat](../l/list_concat.md)
  - [generate_join_implied_equalities](../g/generate_join_implied_equalities.md)
  - [bms_union](../b/bms_union.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [get_plan_rowmark](../g/get_plan_rowmark.md)
  - [predicate_implied_by](../p/predicate_implied_by.md)
  - [contain_mutable_functions](contain_mutable_functions.md)
  - [IndexOptInfo](../I/IndexOptInfo.md) (structure)
  - RELOPT_OTHER_MEMBER_REL (constant)
- Called from (representative examples):
  - [set_plain_rel_size](../s/set_plain_rel_size.md)
  - [set_tablesample_rel_size](../s/set_tablesample_rel_size.md)

## Notes and Other Information
- Only processes base or "other" member relations (asserted via IS_SIMPLE_REL)
- Initializes indrestrictinfo to baserestrictinfo for all indexes initially
- Short-circuits if no partial indexes exist
- For target relations, leaves indrestrictinfo unchanged to ensure proper EvalPlanQual behavior
- Supports re-computation when new restrictions are added, though this rarely happens in core code
- Computes indrestrictinfo even for non-predOK indexes as they may be useful in OR clauses
- File location: src/backend/optimizer/path/indxpath.c:3244-3381

## Simplified Source

This simplified version focuses on the core predicate checking logic:

```c
void check_index_predicates(PlannerInfo *root, RelOptInfo *rel)
{
    List *clauselist;
    bool have_partial;
    bool is_target_rel;
    Relids otherrels;
    ListCell *lc;

    // Only works on base or "other" member relations
    Assert(IS_SIMPLE_REL(rel));

    // Initialize and check for partial indexes
    have_partial = false;
    foreach(lc, rel->indexlist)
    {
        IndexOptInfo *index = (IndexOptInfo *) lfirst(lc);
        index->indrestrictinfo = rel->baserestrictinfo;
        if (index->indpred)
            have_partial = true;
    }
    if (!have_partial)
        return;  /* No partial indexes to process */

    // Build list of clauses we can assume true
    clauselist = list_copy(rel->baserestrictinfo);

    // Add movable join clauses
    foreach(lc, rel->joininfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
        if (join_clause_is_movable_to(rinfo, rel))
            clauselist = lappend(clauselist, rinfo);
    }

    // Add equivalence-derivable join clauses
    if (rel->reloptkind == RELOPT_OTHER_MEMBER_REL)
        otherrels = bms_difference(root->all_query_rels,
                                 find_childrel_parents(root, rel));
    else
        otherrels = bms_difference(root->all_query_rels, rel->relids);
    otherrels = bms_del_members(otherrels, rel->nulling_relids);

    if (!bms_is_empty(otherrels))
        clauselist = list_concat(clauselist,
                               generate_join_implied_equalities(root,
                                                               bms_union(rel->relids, otherrels),
                                                               otherrels, rel, NULL));

    // Check if this is a target relation (affects qual removal)
    is_target_rel = (bms_is_member(rel->relid, root->all_result_relids) ||
                     get_plan_rowmark(root->rowMarks, rel->relid) != NULL);

    // Test each partial index predicate
    foreach(lc, rel->indexlist)
    {
        IndexOptInfo *index = (IndexOptInfo *) lfirst(lc);

        if (index->indpred == NIL)
            continue;  /* skip non-partial indexes */

        // Check if predicate is satisfied
        if (!index->predOK)
            index->predOK = predicate_implied_by(index->indpred, clauselist, false);

        // For target relations, can't remove implied quals
        if (is_target_rel)
            continue;

        // Build restricted qual list (non-implied quals only)
        index->indrestrictinfo = NIL;
        foreach(ListCell *lcr, rel->baserestrictinfo)
        {
            RestrictInfo *rinfo = (RestrictInfo *) lfirst(lcr);

            if (contain_mutable_functions((Node *) rinfo->clause) ||
                !predicate_implied_by(list_make1(rinfo->clause),
                                    index->indpred, false))
                index->indrestrictinfo = lappend(index->indrestrictinfo, rinfo);
        }
    }
}
```

**Key simplifications made:**
- Removed extensive comments while preserving essential logic comments
- Condensed the clauselist building process but maintained all three sources
- Simplified the target relation check explanation
- Preserved all critical safety checks and conditional logic
- Reduced from ~150 lines to ~70 lines while maintaining full functionality