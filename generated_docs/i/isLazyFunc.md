# isLazyFunc

## Location
src/bin/pgbench/pgbench.c: 2125 - 2131

## Overview
A predicate function that determines whether a given PgBench function requires lazy evaluation semantics.

## Definition
```c
static bool isLazyFunc(PgBenchFunction func)
```

## Detailed Description
The `isLazyFunc` function identifies functions in pgbench that require lazy evaluation - meaning their arguments should not all be evaluated immediately but rather evaluated conditionally based on control flow. This is essential for functions like logical operators (AND, OR) and conditional expressions (CASE) where short-circuiting behavior is expected. The function returns true for AND, OR, and CASE operations, which need special handling during expression evaluation to implement proper short-circuit semantics.

## Parameters / Member Variables
- `func`: A `PgBenchFunction` enum value representing the function type to be checked

## Dependencies
- Functions called/Symbols referenced:
  - `PgBenchFunction` (enum type)
  - `PGBENCH_AND` (enum constant)
  - `PGBENCH_OR` (enum constant)
  - `PGBENCH_CASE` (enum constant)
- Called from (representative examples):
  - `evalLazyFunc`
  - `evalFunc`

## Notes and Other Information
- This is a static function with internal linkage, only accessible within pgbench.c
- Supports pgbench's dual evaluation strategy: eager evaluation for most functions and lazy evaluation for control-flow functions
- Critical for implementing proper short-circuit evaluation semantics in pgbench expressions
- The three lazy functions (AND, OR, CASE) are the only ones that require conditional argument evaluation