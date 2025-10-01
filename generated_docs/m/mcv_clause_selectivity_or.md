# mcv_clause_selectivity_or

## Location
[src/backend/statistics/mcv.c:2126-2175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mcv.c#L2126-L2175)

## Overview
Estimates the selectivity of a clause that appears in an ORed list of clauses using MCV (Most Common Values) statistics, providing detailed overlap analysis for accurate combined selectivity calculations.

## Definition

```c
Selectivity
mcv_clause_selectivity_or(PlannerInfo *root, StatisticExtInfo *stat,
						  MCVList *mcv, Node *clause, bool **or_matches,
						  Selectivity *basesel, Selectivity *overlap_mcvsel,
						  Selectivity *overlap_basesel, Selectivity *totalsel)
```
## Detailed Description
This function is a specialized component of PostgreSQL's query planning system that uses MCV statistics to estimate the selectivity of individual clauses within OR expressions. It implements a sophisticated algorithm to handle the overlap between multiple OR clauses, which is essential for accurate selectivity estimation in complex WHERE conditions.

The function operates on the principle that for OR clauses P[n] = P(C[1] OR C[2] OR ... OR C[n]), the combined selectivity can be calculated as:
P[n+1] = P[n] + P(C[n+1]) - P((C[1] OR ... OR C[n]) AND C[n+1])

It maintains a match bitmap () to track which MCV items have been matched by previously processed clauses in the OR list, enabling accurate overlap calculation for the intersection term. The function returns multiple selectivity values to support the overall OR selectivity computation algorithm.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and statistics
- : StatisticExtInfo containing extended statistics information for the relation
- : MCVList structure containing the most common values and their frequencies
- : Node representing the individual clause being analyzed within the OR expression
- : In/out parameter - bitmap tracking MCV items matched by previously processed OR clauses (NULL on first call)
- : Output parameter - sum of base frequencies for MCV items matching this clause
- : Output parameter - sum of MCV frequencies for items in the overlap between this and previous clauses
- : Output parameter - sum of base frequencies for items in the overlap
- : Output parameter - sum of frequencies for all MCV items (used for normalization)

## Dependencies
- Functions called/Symbols referenced:
  - [mcv_get_match_bitmap](mcv_get_match_bitmap.md)
  - [palloc0](../p/palloc0.md)
  - [pfree](../p/pfree.md)
  - list_make1
- Called from (representative examples):
  - [statext_mcv_clauselist_selectivity](../s/statext_mcv_clauselist_selectivity.md)

## Notes and Other Information
This function is part of PostgreSQL's extended statistics framework introduced to improve query planning for correlated columns. It's specifically designed to handle OR clauses more accurately than traditional independence assumptions. The caller must initialize  to NULL on the first call, and the function maintains this bitmap across multiple calls for the same OR expression. The function is located in src/backend/statistics/mcv.c:2126-2175 and works in conjunction with other MCV statistics functions to provide comprehensive selectivity estimation for complex query conditions.

## Simplified Source

```c
Selectivity
mcv_clause_selectivity_or(PlannerInfo *root, StatisticExtInfo *stat,
                          MCVList *mcv, Node *clause, bool **or_matches,
                          Selectivity *basesel, Selectivity *overlap_mcvsel,
                          Selectivity *overlap_basesel, Selectivity *totalsel)
{
    Selectivity s = 0.0;
    bool *new_matches;
    int i;

    // Initialize OR-matches bitmap if not already built
    if (*or_matches == NULL)
        *or_matches = palloc0(sizeof(bool) * mcv->nitems);

    // Build match bitmap for the new clause
    new_matches = mcv_get_match_bitmap(root, list_make1(clause), stat->keys,
                                       stat->exprs, mcv, false);

    // Calculate selectivities for this clause and overlaps
    *basesel = 0.0;
    *overlap_mcvsel = 0.0;
    *overlap_basesel = 0.0;
    *totalsel = 0.0;

    for (i = 0; i < mcv->nitems; i++) {
        *totalsel += mcv->items[i].frequency;

        if (new_matches[i]) {
            // This MCV item matches the new clause
            s += mcv->items[i].frequency;
            *basesel += mcv->items[i].base_frequency;

            // Check for overlap with previous clauses
            if ((*or_matches)[i]) {
                *overlap_mcvsel += mcv->items[i].frequency;
                *overlap_basesel += mcv->items[i].base_frequency;
            }
        }

        // Update OR-matches bitmap for next clause
        (*or_matches)[i] = (*or_matches)[i] || new_matches[i];
    }

    pfree(new_matches);

    return s;
}
```