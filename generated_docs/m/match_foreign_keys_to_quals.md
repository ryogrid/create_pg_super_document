# match_foreign_keys_to_quals

## Location
[src/backend/optimizer/plan/initsplan.c:3209-3373](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L3209-L3373)

## Overview
Matches foreign-key constraints to equivalence classes and join quals to enable more reliable selectivity estimates, especially for multiple-column FKs where independence assumptions typically fail.

## Definition

```c
void
match_foreign_keys_to_quals(PlannerInfo *root)
```
## Detailed Description
This function is a key component of PostgreSQL's cost-based query optimization that leverages foreign key semantics for better selectivity estimation. The core idea is to identify which query join conditions match equality constraints of foreign-key relationships, allowing the optimizer to make more accurate cardinality estimates than would be possible using statistical independence assumptions.

The function processes the ForeignKeyOptInfos in root->fkey_list, annotating them with information about which equivalence classes and join qualification clauses they match. It discards any ForeignKeyOptInfos that are irrelevant for the current query, ensuring that only useful foreign key information is retained for cost estimation.

The matching process involves two main strategies:
1. Matching FK columns to equivalence classes (preferred for simple inner joins)
2. Matching FK columns to "loose" join qualification clauses (for outer joins and complex conditions)

Currently, the function only retains multicolumn FKs that are fully matched to the query, though this may be relaxed in future versions to derive partial estimates.

## Parameters / Member Variables
- : PlannerInfo structure containing all global information about the query being planned, including the foreign key list to be processed

## Dependencies
- Functions called/Symbols referenced:
  - [match_eclasses_to_foreign_key_col](match_eclasses_to_foreign_key_col.md) (matches FK columns to equivalence classes)
  - [get_leftop](../g/get_leftop.md)/get_rightop (extract operands from expressions)
  - [get_commutator](../g/get_commutator.md) (finds commutator operators)
  - [lappend](../l/lappend.md) (list manipulation)
  - [ForeignKeyOptInfo](../F/ForeignKeyOptInfo.md) (structure containing FK optimization information)
  - [EquivalenceClass](../E/EquivalenceClass.md) (structure for equivalence class management)
  - [OpExpr](../O/OpExpr.md) (operator expression node)
  - [RelabelType](../R/RelabelType.md) (type relabeling expression node)
  - RELOPT_BASEREL (enumeration for base relation types)
- Called from:
  - [query_planner](../q/query_planner.md) (main query planning entry point)

## Notes and Other Information
- The function performs extensive validation to ensure both the constraining and referenced relations are base relations present in the query
- It handles both direct and reverse column matches, using commutator operators when necessary
- [RelabelType](../R/RelabelType.md) nodes are stripped away to reach the underlying Var nodes for proper matching
- The function prioritizes equivalence class matches over loose qualification matches
- Foreign keys linking to inheritance child relations (otherrels) are ignored
- The current implementation requires full column matching for multicolumn FKs to be retained
- This optimization is particularly valuable for star-schema and other well-normalized database designs where FK relationships are common

## Simplified Source

```c
void match_foreign_keys_to_quals(PlannerInfo *root) {
    List *newlist = NIL;
    ListCell *lc;

    // Process each foreign key in the list
    foreach(lc, root->fkey_list) {
        ForeignKeyOptInfo *fkinfo = (ForeignKeyOptInfo *) lfirst(lc);
        RelOptInfo *con_rel, *ref_rel;
        int colno;

        // Validate that both FK relations exist and are base relations
        if (fkinfo->con_relid >= root->simple_rel_array_size ||
            fkinfo->ref_relid >= root->simple_rel_array_size)
            continue;

        con_rel = root->simple_rel_array[fkinfo->con_relid];
        ref_rel = root->simple_rel_array[fkinfo->ref_relid];

        if (con_rel == NULL || ref_rel == NULL)
            continue;

        if (con_rel->reloptkind != RELOPT_BASEREL ||
            ref_rel->reloptkind != RELOPT_BASEREL)
            continue;

        // Try to match each FK column to query conditions
        for (colno = 0; colno < fkinfo->nkeys; colno++) {
            EquivalenceClass *ec;

            // First try to match with equivalence classes
            ec = match_eclasses_to_foreign_key_col(root, fkinfo, colno);
            if (ec != NULL) {
                fkinfo->nmatched_ec++;
                if (ec->ec_has_const)
                    fkinfo->nconst_ec++;
                continue;
            }

            // Look for matching join clauses in joininfo list
            AttrNumber con_attno = fkinfo->conkey[colno];
            AttrNumber ref_attno = fkinfo->confkey[colno];
            ListCell *lc2;

            foreach(lc2, con_rel->joininfo) {
                RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc2);
                OpExpr *clause = (OpExpr *) rinfo->clause;

                // Check for binary equality operators
                if (!IsA(clause, OpExpr) || list_length(clause->args) != 2)
                    continue;

                Var *leftvar = (Var *) get_leftop((Expr *) clause);
                Var *rightvar = (Var *) get_rightop((Expr *) clause);

                // Strip RelabelType nodes to get to underlying Vars
                while (leftvar && IsA(leftvar, RelabelType))
                    leftvar = (Var *) ((RelabelType *) leftvar)->arg;
                while (rightvar && IsA(rightvar, RelabelType))
                    rightvar = (Var *) ((RelabelType *) rightvar)->arg;

                if (!(leftvar && IsA(leftvar, Var)) ||
                    !(rightvar && IsA(rightvar, Var)))
                    continue;

                // Check for direct or reverse FK column match
                bool direct_match = (fkinfo->ref_relid == leftvar->varno &&
                                     ref_attno == leftvar->varattno &&
                                     fkinfo->con_relid == rightvar->varno &&
                                     con_attno == rightvar->varattno &&
                                     clause->opno == fkinfo->conpfeqop[colno]);

                bool reverse_match = false;
                if (!direct_match) {
                    Oid commutator_op = get_commutator(fkinfo->conpfeqop[colno]);
                    reverse_match = (fkinfo->ref_relid == rightvar->varno &&
                                     ref_attno == rightvar->varattno &&
                                     fkinfo->con_relid == leftvar->varno &&
                                     con_attno == leftvar->varattno &&
                                     clause->opno == commutator_op);
                }

                if (direct_match || reverse_match) {
                    fkinfo->rinfos[colno] = lappend(fkinfo->rinfos[colno], rinfo);
                    fkinfo->nmatched_ri++;
                }
            }

            // Count column as matched if we found qualifying clauses
            if (fkinfo->rinfos[colno])
                fkinfo->nmatched_rcols++;
        }

        // Only retain fully matched multicolumn FKs
        if ((fkinfo->nmatched_ec + fkinfo->nmatched_rcols) == fkinfo->nkeys)
            newlist = lappend(newlist, fkinfo);
    }

    // Replace the FK list with only useful entries
    root->fkey_list = newlist;
}
```