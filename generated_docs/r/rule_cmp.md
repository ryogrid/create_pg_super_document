# rule_cmp

## Location
src/timezone/zic.c: 2797 - 2810

## Overview
A comparison function for sorting timezone rule structures by high year, month, and day of month in ascending order.

## Definition
```c
static int rule_cmp(struct rule const *a, struct rule const *b)
```

## Detailed Description
The `rule_cmp` function implements a three-level comparison for timezone rule structures, designed for use with sorting algorithms like `qsort`. It compares rules in the following priority order:

1. **High year** (`r_hiyear`): Rules with earlier end years come first
2. **Month** (`r_month`): Within the same year, earlier months come first  
3. **Day of month** (`r_dayofmonth`): Within the same month, earlier days come first

The function handles NULL pointer cases by treating NULL as "less than" any valid rule pointer.

## Parameters / Member Variables
- `a`: Pointer to the first rule structure to compare (can be NULL)
- `b`: Pointer to the second rule structure to compare (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - `rule` (struct type for timezone rules)
- Called from (representative examples):
  - `stringzone` (used for sorting rules when generating timezone strings)

## Notes and Other Information
- Returns -1 if `a` should sort before `b`
- Returns 1 if `a` should sort after `b`  
- Returns 0 if `a` and `b` are equivalent for sorting purposes
- Handles NULL pointers gracefully: NULL sorts before any non-NULL rule
- Uses the `!!` idiom to convert boolean expressions to 0 or 1
- Part of PostgreSQL's timezone compilation system for organizing daylight saving time rules