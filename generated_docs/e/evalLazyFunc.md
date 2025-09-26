# evalLazyFunc

## Location
[src/bin/pgbench/pgbench.c:2132-2241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L2132-L2241)

## Overview
Implements lazy (short-circuit) evaluation for logical and conditional functions in pgbench expressions, including AND, OR, and CASE operations.

## Definition
```c
static bool evalLazyFunc(CState *st, PgBenchFunction func, PgBenchExprLink *args, PgBenchValue *retval)
```

## Detailed Description
The `evalLazyFunc` function provides lazy evaluation semantics for pgbench functions that require conditional argument evaluation. It implements short-circuit logic for AND/OR operations and conditional branching for CASE expressions. For AND operations, if the first argument is false, the second argument is not evaluated. For OR operations, if the first argument is true, the second argument is not evaluated. For CASE expressions, it evaluates conditions sequentially and executes only the branch corresponding to the first true condition, with recursive handling for nested CASE expressions.

## Parameters / Member Variables
- `st`: Pointer to the current client state (`CState`) containing execution context
- `func`: The `PgBenchFunction` enum value specifying which lazy function to evaluate (AND, OR, or CASE)
- `args`: Linked list of expression arguments (`PgBenchExprLink`) to be conditionally evaluated
- `retval`: Pointer to `PgBenchValue` where the result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - `[isLazyFunc](../i/isLazyFunc.md)` (validation)
  - `[evaluateExpr](evaluateExpr.md)` (expression evaluation)
  - `[coerceToBool](../c/coerceToBool.md)` (type conversion)
  - `[valueTruth](../v/valueTruth.md)` (truth value determination)
  - `[setNullValue](../s/setNullValue.md)`, `setBoolValue` (result setting)
  - `PGBENCH_AND`, `PGBENCH_OR`, `PGBENCH_CASE` (enum constants)
  - `PGBT_NULL` (type constant)
- Called from (representative examples):
  - `[evalFunc](evalFunc.md)`
  - `[evalLazyFunc](evalLazyFunc.md)` (recursive calls for nested CASE expressions)

## Notes and Other Information
- This is a static function with internal linkage, only accessible within pgbench.c
- Implements proper SQL-like NULL handling where NULL AND anything yields NULL, and NULL OR anything yields NULL
- Uses recursive calls for handling nested CASE expressions with multiple WHEN clauses
- Critical for performance optimization in pgbench expressions by avoiding unnecessary computations
- The function assumes validation has been done by `isLazyFunc` before being called
- Handles three-valued logic (true/false/null) consistent with SQL semantics