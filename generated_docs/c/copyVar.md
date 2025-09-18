# copyVar

## Location
src/backend/optimizer/plan/setrefs.c: 1956 - 1977

## Overview
A specialized utility function that creates a shallow copy of a Var node, optimized for performance in expression tree manipulation during query planning.

## Definition
```c
static inline Var *copyVar(Var *var)
```

## Detailed Description
The `copyVar` function provides a lightweight alternative to PostgreSQL's generic `copyObject()` function specifically for copying Var nodes. It performs a shallow copy by allocating memory for a new Var structure and copying the entire contents using structure assignment. This specialized function exists because Var node copying is performed frequently during expression tree processing in the optimizer, particularly by functions like `fix_scan_expr` and related routines.

The function is marked as `static inline` to encourage compiler inlining for maximum performance, since it's called repeatedly during query optimization phases.

## Parameters / Member Variables
- `var`: Pointer to the source Var node to be copied

## Dependencies
- Functions called/Symbols referenced:
  - palloc (PostgreSQL memory allocation function)
- Called from (representative examples):
  - fix_scan_expr_mutator
  - search_indexed_tlist_for_var
  - fix_join_expr_mutator

## Notes and Other Information
- Performs a shallow copy using structure assignment (*newvar = *var)
- Optimized specifically for Var nodes rather than using the generic copyObject() framework
- Marked as inline to minimize function call overhead during frequent usage
- Used extensively during expression tree rewriting and reference fixing in the optimizer
- Part of the setrefs.c module which handles plan tree reference resolution