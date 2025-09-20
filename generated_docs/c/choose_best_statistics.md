# choose_best_statistics

## Location
[src/backend/statistics/extended_stats.c:1209-1330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L1209-L1330)

## Overview
Selects the best statistics object from a list based on specified criteria, prioritizing statistics that cover the most attributes and expressions while minimizing the total number of keys.

## Definition

```c
StatisticExtInfo *
choose_best_statistics(List *stats, char requiredkind, bool inh,
					   Bitmapset **clause_attnums, List **clause_exprs,
					   int nclauses)
```
## Detailed Description
This function implements a selection algorithm to choose the most appropriate statistics object for query optimization from a list of available statistics. It uses a two-tier selection criteria: first, it maximizes the number of matched attributes and expressions from unestimated clauses, then breaks ties by preferring statistics objects with fewer total keys. The function iterates through all statistics, filtering by required kind and inheritance flag, then evaluates coverage of clauses using both attribute numbers and expressions. Only statistics objects that match at least two attributes/expressions are considered candidates.

## Parameters / Member Variables
- : List of available StatisticExtInfo objects to choose from
- : Required statistics kind (type) that the chosen statistic must have
- : Inheritance flag that must match the statistics object's inherit flag
- : Array of bitmaps containing attribute numbers for individual clauses (NULL for incompatible/estimated clauses)
- : Array of expression lists for individual clauses (NULL for incompatible/estimated clauses)
- : Number of clauses to evaluate

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_subset](../b/bms_is_subset.md)
  - [stat_covers_expressions](../s/stat_covers_expressions.md)
  - [bms_add_members](../b/bms_add_members.md)
  - [bms_num_members](../b/bms_num_members.md)
  - [bms_free](../b/bms_free.md)
  - list_length
  - STATS_MAX_DIMENSIONS
- Called from (representative examples):
  - [statext_mcv_clauselist_selectivity](../s/statext_mcv_clauselist_selectivity.md)

## Notes and Other Information
The selection algorithm uses a greedy approach with clear prioritization: it first seeks to maximize coverage (number of matched attributes and expressions) and only considers the number of keys as a secondary criterion for tie-breaking. If multiple statistics objects tie on both criteria, the selection depends on the order they appear in the stats list, which may warrant additional tiebreakers in future implementations. The function requires at least two matched attributes/expressions as a minimum threshold for consideration.