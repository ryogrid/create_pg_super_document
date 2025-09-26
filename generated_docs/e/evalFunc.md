# evalFunc

## Location
[src/bin/pgbench/pgbench.c:2821-2836](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L2821-L2836)

## Overview
A dispatcher function that routes pgbench function calls to the appropriate evaluation strategy based on whether lazy or eager evaluation is required.

## Definition
```c
static bool evalFunc(CState *st, PgBenchFunction func, PgBenchExprLink *args, PgBenchValue *retval)
```

## Detailed Description
The `evalFunc` function serves as the main entry point for evaluating pgbench functions within expressions. It implements a dispatch mechanism that determines the appropriate evaluation strategy based on the function type. For functions requiring lazy evaluation (AND, OR, CASE), it delegates to `evalLazyFunc` to handle short-circuit logic and conditional branching. For all other functions, it delegates to `evalStandardFunc` which performs eager evaluation of all arguments before function execution. This design separates concerns and ensures optimal performance by avoiding unnecessary argument evaluation when possible.

## Parameters / Member Variables
- `st`: Pointer to the current client state (`CState`) containing execution context
- `func`: The `PgBenchFunction` enum value specifying which function to evaluate
- `args`: Linked list of expression arguments (`PgBenchExprLink`) to be evaluated
- `retval`: Pointer to `PgBenchValue` where the result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - `[isLazyFunc](../i/isLazyFunc.md)` (determines evaluation strategy)
  - `[evalLazyFunc](evalLazyFunc.md)` (handles lazy evaluation)
  - `[evalStandardFunc](evalStandardFunc.md)` (handles eager evaluation)
  - `[CState](../C/CState.md)`, `PgBenchFunction`, `PgBenchExprLink`, `PgBenchValue` (type definitions)
- Called from (representative examples):
  - `[evaluateExpr](evaluateExpr.md)`

## Notes and Other Information
- This is a static function with internal linkage, only accessible within pgbench.c
- Acts as a simple but critical dispatch point in the expression evaluation pipeline
- The function maintains the separation between lazy and eager evaluation strategies
- Central to pgbench's dual evaluation approach that optimizes performance for different function types
- All function evaluation in pgbench expressions ultimately flows through this dispatcher
- The design allows for easy extension of new evaluation strategies if needed in the future