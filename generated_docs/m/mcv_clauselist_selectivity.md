# mcv_clauselist_selectivity

## Location
src/backend/statistics/mcv.c: 2048 - 2125

## Overview
Uses MCV (Most Common Values) statistics to estimate the selectivity of an implicitly-ANDed list of clauses by finding matching MCV items and summing their frequencies.

## Definition
```c
Selectivity mcv_clauselist_selectivity(PlannerInfo *root, StatisticExtInfo *stat,
                                      List *clauses, int varRelid,
                                      JoinType jointype, SpecialJoinInfo *sjinfo,
                                      RelOptInfo *rel,
                                      Selectivity *basesel, Selectivity *totalsel)
```

## Detailed Description
This function determines which MCV items match every clause in an ANDed list and returns the sum of their frequencies as the selectivity estimate. It loads the MCV list from the statistics object, builds a match bitmap using mcv_get_match_bitmap, and then processes the results to calculate various selectivity components. The function returns not only the main selectivity (sum of matching item frequencies) but also the base selectivity (sum of base frequencies assuming independence) and total selectivity (sum of all MCV item frequencies). These multiple return values enable more sophisticated selectivity estimation when combined with simple per-column estimates.

## Parameters / Member Variables
- `root`: PlannerInfo containing planner context and statistics
- `stat`: StatisticExtInfo containing information about the extended statistics object
- `clauses`: List of clauses to evaluate (implicitly ANDed together)
- `varRelid`: Variable relation ID for the query context
- `jointype`: Type of join operation being planned
- `sjinfo`: Special join information for complex join scenarios
- `rel`: RelOptInfo for the relation being analyzed
- `basesel`: Output parameter for base selectivity (sum of base frequencies of matching items)
- `totalsel`: Output parameter for total selectivity (sum of all MCV item frequencies)

## Dependencies
- Functions called/Symbols referenced:
  - StatisticExtInfo, JoinType, SpecialJoinInfo, MCVList
  - statext_mcv_load, mcv_get_match_bitmap
- Called from (representative examples):
  - statext_mcv_clauselist_selectivity

## Notes and Other Information
- This function processes clauses with implicit AND logic (all clauses must match)
- The multiple selectivity values returned enable sophisticated estimation models
- Uses inheritance flag from RangeTblEntry to load appropriate statistics
- Works in conjunction with mcv_combine_selectivities() for final estimate calculation
- Critical component of PostgreSQL's extended statistics system for multi-column correlation analysis
- Located in src/backend/statistics/mcv.c:2048-2125