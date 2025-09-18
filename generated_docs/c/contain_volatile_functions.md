# contain_volatile_functions

## Location
src/backend/optimizer/util/clauses.c: 538 - 543

## Overview
A recursive function that searches for volatile functions within expression trees, used by the PostgreSQL optimizer to prevent unsafe optimizations like converting volatile expressions into index scan qualifiers.

## Definition
```c
bool contain_volatile_functions(Node *clause)
```

## Detailed Description
This function serves as the main entry point for detecting volatile functions in PostgreSQL expression trees. It performs a comprehensive search that affects critical optimization decisions throughout the query planner and executor.

Key behavioral aspects:
- **SubLink vs SubPlan Handling**: Recursively examines SubLink sub-selects (during query transformation) but deliberately ignores SubPlans (after planning), since executor evaluation rules for SubPlans don't change based on volatility
- **Caching Strategy**: For performance optimization, results are cached in specific node types (RestrictInfo, PathTarget) with volatility states: VOLATILITY_UNKNOWN, VOLATILITY_NOVOLATILE, or VOLATILITY_VOLATILE
- **Planning Context**: Designed for use with expressions that have undergone preprocessing; external callers should typically use `contain_volatile_functions_after_planning()` instead

The function prevents unsafe optimizations such as:
- Converting volatile expressions into index scan conditions
- Duplicating sub-selects containing volatile functions
- Inappropriate constant folding of volatile expressions

## Parameters / Member Variables
- `clause`: The expression tree node to search for volatile functions

## Dependencies
- Functions called/Symbols referenced:
  - contain_volatile_functions_walker
- Called from (representative examples):
  - CopyFrom (commands/copyfrom.c)  
  - ATExecAddColumn (commands/tablecmds.c)
  - match_opclause_to_indexcol (optimizer/path/indxpath.c)
  - is_pseudo_constant_clause (optimizer/util/clauses.c)
  - subquery_planner (optimizer/plan/planner.c)

## Notes and Other Information
- Returns `true` if any volatile function is found, `false` otherwise
- Part of the broader volatility analysis framework critical for query correctness
- Used extensively throughout the optimizer for index usage decisions, join planning, and subquery optimization
- Cache invalidation responsibility lies with code that modifies nodes - they must reset cached values to VOLATILITY_UNKNOWN