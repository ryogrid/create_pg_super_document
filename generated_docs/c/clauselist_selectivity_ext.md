# clauselist_selectivity_ext

## Location
[src/backend/optimizer/path/clausesel.c:117-358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/clausesel.c#L117-L358)

## Overview
Extended version of clauselist_selectivity that provides fine-grained control over extended statistics usage and implements sophisticated selectivity estimation for ANDed clauses, including range query optimization and extended statistics correlation analysis.

## Definition
```c
Selectivity clauselist_selectivity_ext(PlannerInfo *root,
                                       List *clauses,
                                       int varRelid,
                                       JoinType jointype,
                                       SpecialJoinInfo *sjinfo,
                                       bool use_extended_stats)
```

## Detailed Description
This is the core implementation of PostgreSQL's clause selectivity estimation algorithm. The function processes a list of ANDed boolean clauses using multiple sophisticated techniques:

1. **Single Clause Optimization**: For single-clause lists, delegates directly to `clause_selectivity_ext` to avoid unnecessary overhead.

2. **Extended Statistics Application**: When `use_extended_stats` is true and clauses reference a single relation with extended statistics, applies cross-column correlation analysis via `statext_clauselist_selectivity`.

3. **Range Query Recognition**: Identifies and optimizes range queries (e.g., "x > 34 AND x < 42") by:
   - Recognizing restriction opclauses with scalar comparison operators
   - Pairing clauses that reference the same variable 
   - Using the formula: hisel + losel - 1 + null_frac instead of hisel * losel
   - Handling redundant inequalities by keeping only the tighter constraint

4. **Fallback Processing**: For unmatched clauses, applies standard independence assumption (multiplication of selectivities).

5. **Robustness Features**: Includes safeguards for default selectivity estimates, roundoff errors, and impossible negative results.

The algorithm significantly improves selectivity estimation accuracy compared to naive independence assumptions, especially for correlated columns and range predicates.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning context, relation statistics, and metadata
- `clauses`: List of boolean expression clauses to be ANDed together (can be empty or single-element)
- `varRelid`: Variable relation ID for parameterized path estimation context
- `jointype`: Join type affecting selectivity calculation methodology
- `sjinfo`: Special join information for outer join and semi-join handling
- `use_extended_stats`: Boolean flag to enable/disable extended statistics usage for cross-column correlations

## Dependencies
- Functions called/Symbols referenced:
  - [clause_selectivity_ext](clause_selectivity_ext.md)
  - [find_single_rel_for_clauses](../f/find_single_rel_for_clauses.md)
  - [statext_clauselist_selectivity](../s/statext_clauselist_selectivity.md)
  - [addRangeClause](../a/addRangeClause.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [is_opclause](../i/is_opclause.md)
  - [is_pseudo_constant_clause_relids](../i/is_pseudo_constant_clause_relids.md)
  - [is_pseudo_constant_clause](../i/is_pseudo_constant_clause.md)
  - [get_oprrest](../g/get_oprrest.md)
  - [nulltestsel](../n/nulltestsel.md)
  - [NumRelids](../N/NumRelids.md)
  - RTE_RELATION
  - DEFAULT_INEQ_SEL
  - DEFAULT_RANGE_INEQ_SEL
  - IS_NULL
- Called from (representative examples):
  - [clauselist_selectivity](clauselist_selectivity.md)
  - [clause_selectivity_ext](clause_selectivity_ext.md)
  - [clauselist_apply_dependencies](clauselist_apply_dependencies.md)
  - [statext_mcv_clauselist_selectivity](../s/statext_mcv_clauselist_selectivity.md)

## Notes and Other Information
This function represents the heart of PostgreSQL's selectivity estimation engine and is critical for accurate cost-based query optimization. Key implementation notes:

- [Range](../R/Range.md) query optimization uses the insight that for "x > a AND x < b", the selectivity is the overlap of ranges rather than their product
- Extended statistics integration allows capturing real-world column correlations that independence assumptions miss
- The algorithm maintains a range query list (rqlist) to track and pair comparison operators
- Handles edge cases like pseudoconstant clauses, default selectivity punting, and numerical precision issues
- Performance optimization includes early single-clause detection and bitmap-based clause tracking
- The function is extensively used throughout the optimizer for cost estimation and plan selection

## Simplified Source

```c
Selectivity clauselist_selectivity_ext(PlannerInfo *root,
                                       List *clauses,
                                       int varRelid,
                                       JoinType jointype,
                                       SpecialJoinInfo *sjinfo,
                                       bool use_extended_stats)
{
    Selectivity s1 = 1.0;
    RelOptInfo *rel;
    Bitmapset *estimatedclauses = NULL;
    RangeQueryClause *rqlist = NULL;
    ListCell *l;
    int listidx;

    // Optimize single clause case
    if (list_length(clauses) == 1)
        return clause_selectivity_ext(root, (Node *) linitial(clauses),
                                      varRelid, jointype, sjinfo,
                                      use_extended_stats);

    // Try using extended statistics for multi-column correlations
    rel = find_single_rel_for_clauses(root, clauses);
    if (use_extended_stats && rel && rel->rtekind == RTE_RELATION && rel->statlist != NIL) {
        s1 = statext_clauselist_selectivity(root, clauses, varRelid,
                                             jointype, sjinfo, rel,
                                             &estimatedclauses, false);
    }

    // Process remaining clauses not handled by extended stats
    listidx = -1;
    foreach(l, clauses) {
        Node *clause = (Node *) lfirst(l);
        RestrictInfo *rinfo;
        Selectivity s2;

        listidx++;

        // Skip clauses already estimated by extended statistics
        if (bms_is_member(listidx, estimatedclauses))
            continue;

        // Calculate selectivity for this clause
        s2 = clause_selectivity_ext(root, clause, varRelid, jointype, sjinfo,
                                    use_extended_stats);

        // Handle RestrictInfo wrapper
        if (IsA(clause, RestrictInfo)) {
            rinfo = (RestrictInfo *) clause;
            if (rinfo->pseudoconstant) {
                s1 = s1 * s2;
                continue;
            }
            clause = (Node *) rinfo->clause;
        } else
            rinfo = NULL;

        // Check for range query clauses (x > a AND x < b)
        if (is_opclause(clause) && list_length(((OpExpr *) clause)->args) == 2) {
            OpExpr *expr = (OpExpr *) clause;
            bool varonleft = true;
            bool is_range_clause = false;

            // Verify it's a single-relation comparison with a constant
            if (rinfo) {
                is_range_clause = (rinfo->num_base_rels == 1) &&
                    (is_pseudo_constant_clause_relids(lsecond(expr->args), rinfo->right_relids) ||
                     (varonleft = false, is_pseudo_constant_clause_relids(linitial(expr->args), rinfo->left_relids)));
            } else {
                is_range_clause = (NumRelids(root, clause) == 1) &&
                    (is_pseudo_constant_clause(lsecond(expr->args)) ||
                     (varonleft = false, is_pseudo_constant_clause(linitial(expr->args))));
            }

            if (is_range_clause) {
                // Add to range query list for paired processing
                switch (get_oprrest(expr->opno)) {
                    case F_SCALARLTSEL:
                    case F_SCALARLESEL:
                        addRangeClause(&rqlist, clause, varonleft, true, s2);
                        break;
                    case F_SCALARGTSEL:
                    case F_SCALARGESEL:
                        addRangeClause(&rqlist, clause, varonleft, false, s2);
                        break;
                    default:
                        s1 = s1 * s2;
                        break;
                }
                continue;
            }
        }

        // Not a range clause, use standard multiplication
        s1 = s1 * s2;
    }

    // Process range query pairs for better estimates
    while (rqlist != NULL) {
        RangeQueryClause *rqnext;

        if (rqlist->have_lobound && rqlist->have_hibound) {
            // Found matching pair: use range formula instead of multiplication
            Selectivity s2;

            if (rqlist->hibound == DEFAULT_INEQ_SEL ||
                rqlist->lobound == DEFAULT_INEQ_SEL) {
                s2 = DEFAULT_RANGE_INEQ_SEL;
            } else {
                // Range selectivity: hibound + lobound - 1 + null_fraction
                s2 = rqlist->hibound + rqlist->lobound - 1.0;
                s2 += nulltestsel(root, IS_NULL, rqlist->var, varRelid, jointype, sjinfo);

                // Handle numerical issues
                if (s2 <= 0.0) {
                    s2 = (s2 < -0.01) ? DEFAULT_RANGE_INEQ_SEL : 1.0e-10;
                }
            }
            s1 *= s2;
        } else {
            // Unpaired range clause, use standard estimate
            if (rqlist->have_lobound)
                s1 *= rqlist->lobound;
            else
                s1 *= rqlist->hibound;
        }

        // Move to next range clause
        rqnext = rqlist->next;
        pfree(rqlist);
        rqlist = rqnext;
    }

    return s1;
}
```