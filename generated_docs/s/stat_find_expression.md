# stat_find_expression

## Location
[src/backend/statistics/extended_stats.c:1141-1167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L1141-L1167)

## Overview
Searches for a specific expression within a statistics object's list of expressions and returns its index position.

## Definition

```c
static int
stat_find_expression(StatisticExtInfo *stat, Node *expr)
```
## Detailed Description
This static function performs a linear search through a statistics object's expression list to locate a specific expression node. It uses PostgreSQL's equal() function to perform deep structural comparison between expression trees. The function is essential for extended statistics operations that involve expressions rather than simple column references, allowing the system to determine if a particular expression is covered by existing statistical data.

## Parameters / Member Variables
- `*stat`: StatisticExtInfo structure containing the statistics object with its expression list
- `*expr`: Node representing the expression to search for in the statistics object
## Dependencies
- Functions called/Symbols referenced:
  - [equal](../e/equal.md) (PostgreSQL node comparison function)
  - [StatisticExtInfo](../S/StatisticExtInfo.md) (type)
  - [Node](../N/Node.md) (PostgreSQL expression tree node type)
  - [List](../L/List.md) operations (foreach, lfirst)
- Called from (representative examples):
  - [stat_covers_expressions](stat_covers_expressions.md)

## Notes and Other Information
- Returns the zero-based index of the expression if found, or -1 if not found
- Uses deep structural comparison via equal() function rather than pointer comparison
- Static function scope - only accessible within the extended_stats.c compilation unit
- Essential for expression-based extended statistics where statistics are collected on computed expressions
- Part of the infrastructure for matching query expressions to available statistics
- Linear search implementation suitable for typical small expression lists in statistics objects
- Located in src/backend/statistics/extended_stats.c:1141-1167

## Simplified Source

```c
static int
stat_find_expression(StatisticExtInfo *stat, Node *expr)
{
    ListCell *lc;
    int idx;

    // Linear search through the statistics object's expression list
    idx = 0;
    foreach(lc, stat->exprs) {
        Node *stat_expr = (Node *) lfirst(lc);

        // Use deep structural comparison to match expressions
        if (equal(stat_expr, expr))
            return idx;
        idx++;
    }

    // Expression not found in statistics object
    return -1;
}
```