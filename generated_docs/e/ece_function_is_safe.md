# ece_function_is_safe

## Location
[src/backend/optimizer/util/clauses.c:3752-3789](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L3752-L3789)

## Overview
A safety check function that determines whether a PostgreSQL function can be safely evaluated during constant expression optimization based on its volatility characteristics.

## Definition

```c
static bool
ece_function_is_safe(Oid funcid, eval_const_expressions_context *context)
```
## Detailed Description
This function serves as a subroutine for  to determine if a function is safe to evaluate during query planning and optimization. It implements PostgreSQL's volatility-based safety model for function evaluation.

The function uses PostgreSQL's function volatility classification system to make safety decisions. Immutable functions are always considered safe for evaluation since their results never change. Stable functions are conditionally safe - they can be evaluated during estimation phases (when  is true) because the potential risk of the result changing between planning and execution time is acceptable when the alternative is having no estimate at all.

Volatile functions are never considered safe for constant expression evaluation as they can return different results on each call, even with identical parameters.

## Parameters / Member Variables
- `funcid`: The OID (object identifier) of the function to check for evaluation safety
- `*context`: Pointer to evaluation context containing flags that influence safety decisions, particularly the  flag
## Dependencies
- Functions called/Symbols referenced:
  -  - Retrieves the volatility classification of the specified function
  -  - Constant indicating immutable function volatility
  -  - Constant indicating stable function volatility
  -  - Context structure type
- Called from (representative examples):
  -  - Main constant expression evaluation function
  -  - Used in parallel query hazard assessment

## Notes and Other Information
- This is a static function, limiting its scope to the clauses.c file
- Implements a risk-benefit trade-off for stable functions during estimation
- Returns true only for immutable functions or stable functions during estimation contexts
- The decision to allow stable function evaluation during estimation reflects PostgreSQL's pragmatic approach to query optimization
- Critical for maintaining query plan determinism while enabling effective cost estimation

## Simplified Source

```c
static bool ece_function_is_safe(Oid funcid, eval_const_expressions_context *context) {
    // Get function's volatility level
    char provolatile = func_volatile(funcid);

    // Always safe to evaluate immutable functions
    if (provolatile == PROVOLATILE_IMMUTABLE)
        return true;

    // Stable functions are safe during estimation
    if (context->estimate && provolatile == PROVOLATILE_STABLE)
        return true;

    // Volatile functions are never safe
    return false;
}
```