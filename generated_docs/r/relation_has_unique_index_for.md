# relation_has_unique_index_for

## Location
[src/backend/optimizer/path/indxpath.c:3440-3613](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L3440-L3613)

## Overview
Determines whether a relation provably has at most one row satisfying a set of equality conditions by checking if the conditions constrain all columns of some unique index.

## Definition

```c
bool
relation_has_unique_index_for(PlannerInfo *root, RelOptInfo *rel,
							  List *restrictlist,
							  List *exprlist, List *oprlist)
```
## Detailed Description
This function is a core component of PostgreSQL's uniqueness analysis that determines if a given set of equality conditions can guarantee at most one matching row from a relation. It works by checking whether the conditions collectively constrain all columns of any unique index on the relation.

The function accepts conditions in two formats: RestrictInfo nodes (for join-derived conditions) and expression/operator pairs. It automatically incorporates usable baserestrictinfo clauses and performs comprehensive matching against all available unique indexes. For each unique index, it verifies that every key column is constrained by an appropriate equality condition with compatible operators from the index's opfamily.

## Parameters / Member Variables
- : PlannerInfo structure containing global query information
- : RelOptInfo structure representing the target relation
- : List of RestrictInfo nodes representing equality conditions (destructively modified)
- : List of expressions in the relation for equality matching
- : List of equality operators corresponding to exprlist expressions

## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md)
  - bms_is_empty
  - [lappend](../l/lappend.md)
  - [list_member_oid](../l/list_member_oid.md)
  - [get_rightop](../g/get_rightop.md)
  - [get_leftop](../g/get_leftop.md)
  - [match_index_to_operand](../m/match_index_to_operand.md)
  - forboth
  - lfirst_oid
  - [op_in_opfamily](../o/op_in_opfamily.md)
  - [IndexOptInfo](../I/IndexOptInfo.md) (structure)
  - [RestrictInfo](../R/RestrictInfo.md) (structure)
- Called from (representative examples):
  - [rel_is_distinct_for](rel_is_distinct_for.md)
  - [create_unique_path](../c/create_unique_path.md)

## Notes and Other Information
- Automatically adds usable baserestrictinfo clauses to the analysis
- Only considers unique, immediately enforced, non-partial indexes
- Cannot use partial unique indexes even if predOK due to join predicate dependencies in check_index_predicates()
- Performs O(N^2) matching between conditions and index columns, assuming short lists
- Currently assumes all collations reduce to the same notion of equality (XXX comment indicates future enhancement needed)
- The restrictlist parameter is destructively modified during processing
- Returns true if any unique index has all its key columns constrained by the provided conditions
- File location: src/backend/optimizer/path/indxpath.c:3440-3613

## Simplified Source

```c
bool
relation_has_unique_index_for(PlannerInfo *root, RelOptInfo *rel,
                              List *restrictlist, List *exprlist, List *oprlist)
{
    ListCell *ic;

    Assert(list_length(exprlist) == list_length(oprlist));

    // No indexes = no uniqueness guarantee
    if (rel->indexlist == NIL)
        return false;

    // Add usable base restriction clauses to restrictlist
    foreach(ic, rel->baserestrictinfo) {
        RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(ic);

        if (restrictinfo->mergeopfamilies == NIL)
            continue; // Not mergejoinable

        // Check if either side is pseudoconstant
        if (bms_is_empty(restrictinfo->left_relids)) {
            restrictinfo->outer_is_left = true;
        }
        else if (bms_is_empty(restrictinfo->right_relids)) {
            restrictinfo->outer_is_left = false;
        }
        else
            continue;

        restrictlist = lappend(restrictlist, restrictinfo);
    }

    // No conditions = no uniqueness possible
    if (restrictlist == NIL && exprlist == NIL)
        return false;

    // Check each unique index
    foreach(ic, rel->indexlist) {
        IndexOptInfo *ind = (IndexOptInfo *) lfirst(ic);
        int c;

        // Skip non-unique, deferred, or partial indexes
        if (!ind->unique || !ind->immediate || ind->indpred != NIL)
            continue;

        // Check if all index key columns are constrained
        for (c = 0; c < ind->nkeycolumns; c++) {
            bool matched = false;
            ListCell *lc, *lc2;

            // Check restrictlist conditions
            foreach(lc, restrictlist) {
                RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
                Node *rexpr;

                // Operator must be in index opfamily
                if (!list_member_oid(rinfo->mergeopfamilies, ind->opfamily[c]))
                    continue;

                // Extract the operand for this relation
                if (rinfo->outer_is_left)
                    rexpr = get_rightop(rinfo->clause);
                else
                    rexpr = get_leftop(rinfo->clause);

                // Check if operand matches this index column
                if (match_index_to_operand(rexpr, c, ind)) {
                    matched = true;
                    break;
                }
            }

            if (matched)
                continue;

            // Check expression/operator list
            forboth(lc, exprlist, lc2, oprlist) {
                Node *expr = (Node *) lfirst(lc);
                Oid opr = lfirst_oid(lc2);

                if (!match_index_to_operand(expr, c, ind))
                    continue;

                // Operator must be in index opfamily
                if (!op_in_opfamily(opr, ind->opfamily[c]))
                    continue;

                matched = true;
                break;
            }

            if (!matched)
                break; // This index can't help
        }

        // All key columns matched for this index
        if (c == ind->nkeycolumns)
            return true;
    }

    return false;
}
```