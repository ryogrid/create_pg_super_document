# stat_covers_expressions

## Location
[src/backend/statistics/extended_stats.c:1168-1208](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L1168-L1208)

## Overview
Tests whether a statistics object covers all expressions in a given list, returning true if all expressions are covered by the statistic.

## Definition

```c
static bool
stat_covers_expressions(StatisticExtInfo *stat, List *exprs,
						Bitmapset **expr_idxs)
```
## Detailed Description
This static function evaluates whether a statistics object contains all the expressions from a provided list. It iterates through each expression in the input list and uses  to locate the expression within the statistics object. If any expression is not found, the function returns false immediately. When all expressions are successfully located, it returns true. The function also optionally populates a bitmap set with the indexes of the found expressions if the  parameter is provided.

## Parameters / Member Variables
- : Pointer to StatisticExtInfo structure containing the statistics object to check
- : List of Node expressions to verify coverage for
- : Optional output parameter - if non-NULL, populated with bitmap of expression indexes found in the statistics object

## Dependencies
- Functions called/Symbols referenced:
  - [stat_find_expression](stat_find_expression.md)
  - [bms_add_member](../b/bms_add_member.md)
  - StatisticExtInfo
- Called from (representative examples):
  - [choose_best_statistics](../c/choose_best_statistics.md)
  - [statext_mcv_clauselist_selectivity](statext_mcv_clauselist_selectivity.md)

## Notes and Other Information
This function is part of the extended statistics infrastructure in PostgreSQL and is used to determine if a particular statistics object can provide useful information for a set of expressions. It's typically called during query planning to identify which statistics objects are applicable for selectivity estimation.