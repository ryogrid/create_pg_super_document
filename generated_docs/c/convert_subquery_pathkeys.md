# convert_subquery_pathkeys

## Location
[src/backend/optimizer/path/pathkeys.c:1052-1248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L1052-L1248)

## Overview
Converts a subquery's output pathkeys into equivalent pathkeys in the context of the outer query, handling volatile expressions and multiple equivalence class representations.

## Definition

```c
List *
convert_subquery_pathkeys(PlannerInfo *root, RelOptInfo *rel,
						  List *subquery_pathkeys,
						  List *subquery_tlist)
```
## Detailed Description
This function performs the complex task of translating pathkeys from a subquery's internal representation to pathkeys that are meaningful in the outer query's context. It handles two main cases:

1. **Volatile EquivalenceClasses**: These must come from ORDER BY clauses and are matched directly to specific targetlist entries using sortref information.

2. **Non-volatile EquivalenceClasses**: These may contain multiple equivalent expressions and require scoring to select the best representation in the outer query context.

For non-volatile classes, the function evaluates each possible representation by counting equivalence class members and checking alignment with outer query pathkeys. It preserves the raw ordering information rather than truncating it, which helps with merge join direction decisions.

The conversion process stops when a subquery pathkey cannot be represented in the outer query, as subsequent pathkeys would also be unusable.

## Parameters / Member Variables
- : PlannerInfo containing the outer query's planning context and equivalence classes
- : RelOptInfo representing the subquery relation in the outer query
- : List of PathKey objects representing the subquery's output ordering
- : The subquery's target list for matching expressions to outer query variables

## Dependencies
- Functions called/Symbols referenced:
  - [get_sortgroupref_tle](../g/get_sortgroupref_tle.md) (to find targetlist entries by sortref)
  - [find_var_for_subquery_tle](../f/find_var_for_subquery_tle.md) (to map subquery outputs to outer query variables)
  - [get_eclass_for_sort_expr](../g/get_eclass_for_sort_expr.md) (to find or create equivalence classes)
  - [make_canonical_pathkey](../m/make_canonical_pathkey.md) (to create standardized pathkeys)
  - [canonicalize_ec_expression](canonicalize_ec_expression.md) (to normalize expressions for comparison)
  - [equal](../e/equal.md) (for expression equality testing)
  - [pathkey_is_redundant](../p/pathkey_is_redundant.md) (to eliminate duplicate ordering information)
- Called from (representative examples):
  - [set_subquery_pathlist](../s/set_subquery_pathlist.md)
  - [set_cte_pathlist](../s/set_cte_pathlist.md)
  - [build_setop_child_paths](../b/build_setop_child_paths.md)

## Notes and Other Information
- Intentionally preserves raw ordering information instead of truncating useless pathkeys
- Uses a scoring system to select the best representation when multiple options exist
- Handles volatile expressions specially due to their ORDER BY clause origins
- Essential for subquery optimization and proper merge join planning
- Part of PostgreSQL's pathkey propagation system for maintaining sort order information across query levels

## Simplified Source

```c
List *
convert_subquery_pathkeys(PlannerInfo *root, RelOptInfo *rel,
                          List *subquery_pathkeys,
                          List *subquery_tlist)
{
    List *retval = NIL;
    int retvallen = 0;
    int outer_query_keys = list_length(root->query_pathkeys);
    ListCell *i;

    foreach(i, subquery_pathkeys) {
        PathKey *sub_pathkey = (PathKey *) lfirst(i);
        EquivalenceClass *sub_eclass = sub_pathkey->pk_eclass;
        PathKey *best_pathkey = NULL;

        if (sub_eclass->ec_has_volatile) {
            // Handle volatile expressions from ORDER BY clauses
            TargetEntry *tle;
            Var *outer_var;

            if (sub_eclass->ec_sortref == 0)
                elog(ERROR, "volatile EquivalenceClass has no sortref");

            tle = get_sortgroupref_tle(sub_eclass->ec_sortref, subquery_tlist);
            outer_var = find_var_for_subquery_tle(rel, tle);

            if (outer_var) {
                EquivalenceMember *sub_member = (EquivalenceMember *) linitial(sub_eclass->ec_members);
                EquivalenceClass *outer_ec = get_eclass_for_sort_expr(root,
                    (Expr *) outer_var, sub_eclass->ec_opfamilies,
                    sub_member->em_datatype, sub_eclass->ec_collation,
                    0, rel->relids, false);

                if (outer_ec)
                    best_pathkey = make_canonical_pathkey(root, outer_ec,
                        sub_pathkey->pk_opfamily, sub_pathkey->pk_strategy,
                        sub_pathkey->pk_nulls_first);
            }
        } else {
            // Handle non-volatile equivalence classes with scoring
            int best_score = -1;
            ListCell *j;

            foreach(j, sub_eclass->ec_members) {
                EquivalenceMember *sub_member = (EquivalenceMember *) lfirst(j);
                Expr *sub_expr = sub_member->em_expr;
                ListCell *k;

                if (sub_member->em_is_child)
                    continue;

                foreach(k, subquery_tlist) {
                    TargetEntry *tle = (TargetEntry *) lfirst(k);
                    Var *outer_var = find_var_for_subquery_tle(rel, tle);
                    if (!outer_var)
                        continue;

                    Expr *tle_expr = canonicalize_ec_expression(tle->expr,
                        sub_member->em_datatype, sub_eclass->ec_collation);
                    if (!equal(tle_expr, sub_expr))
                        continue;

                    EquivalenceClass *outer_ec = get_eclass_for_sort_expr(root,
                        (Expr *) outer_var, sub_eclass->ec_opfamilies,
                        sub_member->em_datatype, sub_eclass->ec_collation,
                        0, rel->relids, false);

                    if (!outer_ec)
                        continue;

                    PathKey *outer_pk = make_canonical_pathkey(root, outer_ec,
                        sub_pathkey->pk_opfamily, sub_pathkey->pk_strategy,
                        sub_pathkey->pk_nulls_first);

                    // Score based on equivalence peers and query pathkey match
                    int score = list_length(outer_ec->ec_members) - 1;
                    if (retvallen < outer_query_keys &&
                        list_nth(root->query_pathkeys, retvallen) == outer_pk)
                        score++;

                    if (score > best_score) {
                        best_pathkey = outer_pk;
                        best_score = score;
                    }
                }
            }
        }

        // Stop if no representation found
        if (!best_pathkey)
            break;

        // Add if not redundant
        if (!pathkey_is_redundant(best_pathkey, retval)) {
            retval = lappend(retval, best_pathkey);
            retvallen++;
        }
    }

    return retval;
}
```