# statext_mcv_clauselist_selectivity

## Location
[src/backend/statistics/extended_stats.c:1696-1983](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L1696-L1983)

## Overview
Estimates clause selectivity using the best multi-column MCV (Most Common Values) statistics through a greedy algorithm that iteratively applies available statistics to maximize coverage.

## Definition

```c
static Selectivity
statext_mcv_clauselist_selectivity(PlannerInfo *root, List *clauses, int varRelid,
								   JoinType jointype, SpecialJoinInfo *sjinfo,
								   RelOptInfo *rel, Bitmapset **estimatedclauses,
								   bool is_or)
```
## Detailed Description
This function implements a sophisticated selectivity estimation algorithm using extended multi-column MCV statistics. It employs a greedy approach, iteratively selecting the best statistics object that covers the most remaining clauses and applying it to estimate their combined selectivity. The function handles both AND-ed and OR-ed clause lists differently: for AND clauses, it multiplies selectivities together, while for OR clauses it uses the inclusion-exclusion principle to account for overlaps. The algorithm combines simple selectivity (assuming column independence), MCV selectivity (from actual statistics), base selectivity, and total selectivity using mcv_combine_selectivities to produce accurate estimates that leverage both traditional and extended statistics.

## Parameters / Member Variables
- : PlannerInfo structure containing planning context and optimizer state
- : List of restriction clauses to estimate selectivity for
- : Variable relation ID (0 if this is a join relation)
- : Type of join if this is a join selectivity estimation
- : Special join information for outer joins
- : RelOptInfo structure containing relation information and statistics
- : Input/output bitmap tracking which clauses have been estimated (0-based indexes)
- : Boolean flag indicating whether clauses are OR-ed (true) or AND-ed (false)

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - [has_stats_of_kind](../h/has_stats_of_kind.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [statext_is_compatible_clause](statext_is_compatible_clause.md)
  - [choose_best_statistics](../c/choose_best_statistics.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - [stat_covers_expressions](stat_covers_expressions.md)
  - [bms_membership](../b/bms_membership.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_free](../b/bms_free.md)
  - [list_free](../l/list_free.md)
  - [statext_mcv_load](statext_mcv_load.md)
  - [clause_selectivity_ext](../c/clause_selectivity_ext.md)
  - [clauselist_selectivity_ext](../c/clauselist_selectivity_ext.md)
  - [mcv_clause_selectivity_or](../m/mcv_clause_selectivity_or.md)
  - [mcv_clauselist_selectivity](../m/mcv_clauselist_selectivity.md)
  - [mcv_combine_selectivities](../m/mcv_combine_selectivities.md)
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - [statext_clauselist_selectivity](statext_clauselist_selectivity.md)

## Notes and Other Information
The function uses a two-phase approach: first, it preprocesses clauses to extract attribute numbers and expressions, then it iteratively applies the best available statistics. For OR clauses, it implements the complex inclusion-exclusion formula P(A OR B) = P(A) + P(B) - P(A AND B) iteratively. The algorithm prioritizes simple single-column clauses by using traditional selectivity estimates for them while leveraging multi-column statistics for complex clauses. The greedy selection ensures that statistics with the most coverage are applied first, maximizing the benefit of extended statistics while falling back to traditional methods for uncovered clauses.

## Simplified Source

```c
static Selectivity
statext_mcv_clauselist_selectivity(PlannerInfo *root, List *clauses, int varRelid,
                                   JoinType jointype, SpecialJoinInfo *sjinfo,
                                   RelOptInfo *rel, Bitmapset **estimatedclauses,
                                   bool is_or)
{
    Selectivity sel = (is_or) ? 0.0 : 1.0;

    // Check if we have MCV statistics available
    if (!has_stats_of_kind(rel->statlist, STATS_EXT_MCV))
        return sel;

    // Extract attributes and expressions from each clause
    int num_clauses = list_length(clauses);
    Bitmapset **clause_attrs = (Bitmapset **) palloc(sizeof(Bitmapset *) * num_clauses);
    List **clause_exprs = (List **) palloc(sizeof(Node *) * num_clauses);

    // Preprocess clauses to identify compatible ones
    int idx = 0;
    foreach(cell, clauses) {
        Node *clause = (Node *) lfirst(cell);
        if (!bms_is_member(idx, *estimatedclauses) &&
            statext_is_compatible_clause(root, clause, rel->relid,
                                         &clause_attrs[idx], &clause_exprs[idx])) {
            // Keep this clause for processing
        } else {
            clause_attrs[idx] = NULL;
            clause_exprs[idx] = NIL;
        }
        idx++;
    }

    // Iteratively apply best available statistics
    while (true) {
        // Find best statistics object for remaining clauses
        StatisticExtInfo *stat = choose_best_statistics(rel->statlist, STATS_EXT_MCV,
                                                       rte->inh, clause_attrs, clause_exprs,
                                                       num_clauses);
        if (!stat)
            break;

        // Build list of clauses covered by this statistic
        List *stat_clauses = NIL;
        Bitmapset *simple_clauses = NULL;

        idx = 0;
        foreach(cell, clauses) {
            if (!clause_attrs[idx] && !clause_exprs[idx])
                continue; // Already processed or incompatible

            // Check if this statistic covers the clause
            if (bms_is_subset(clause_attrs[idx], stat->keys) &&
                stat_covers_expressions(stat, clause_exprs[idx], NULL)) {

                stat_clauses = lappend(stat_clauses, lfirst(cell));
                *estimatedclauses = bms_add_member(*estimatedclauses, idx);

                // Mark simple clauses (single column/expression)
                if (is_simple_clause(clause_attrs[idx], clause_exprs[idx]))
                    simple_clauses = bms_add_member(simple_clauses, list_length(stat_clauses) - 1);

                // Clean up for next iteration
                clause_attrs[idx] = NULL;
                clause_exprs[idx] = NULL;
            }
            idx++;
        }

        // Apply this statistic to estimate selectivity
        if (is_or) {
            // OR logic: P(A OR B) = P(A) + P(B) - P(A AND B)
            Selectivity stat_sel = estimate_or_selectivity(root, stat, stat_clauses,
                                                          simple_clauses, varRelid,
                                                          jointype, sjinfo, rel);
            sel = sel + stat_sel - sel * stat_sel;
        } else {
            // AND logic: multiply selectivities
            Selectivity simple_sel = clauselist_selectivity_ext(root, stat_clauses,
                                                               varRelid, jointype,
                                                               sjinfo, false);
            Selectivity mcv_sel, mcv_basesel, mcv_totalsel;
            mcv_sel = mcv_clauselist_selectivity(root, stat, stat_clauses,
                                                varRelid, jointype, sjinfo, rel,
                                                &mcv_basesel, &mcv_totalsel);

            Selectivity stat_sel = mcv_combine_selectivities(simple_sel, mcv_sel,
                                                            mcv_basesel, mcv_totalsel);
            sel *= stat_sel;
        }
    }

    return sel;
}
```