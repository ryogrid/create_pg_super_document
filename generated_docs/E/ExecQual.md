# ExecQual

## Location
src/include/executor/executor.h: 414 - 440

## Overview
ExecQual evaluates a qualification (boolean) expression and returns true or false, serving as the core function for WHERE clause evaluation and other boolean condition checking throughout PostgreSQL's executor.

## Definition

```c
static inline bool
ExecQual(ExprState *state, ExprContext *econtext)
```
## Detailed Description
ExecQual is a critical inline function in PostgreSQL's expression evaluation system that processes qualification expressions (boolean expressions typically used in WHERE clauses, JOIN conditions, and CHECK constraints). The function takes a compiled expression state and an expression context, evaluates the expression, and returns a boolean result.

The function implements several important optimizations and safety checks:
- Short-circuits immediately if the expression state is NULL (empty restriction list), returning true
- Verifies that the expression was properly compiled with the EEO_FLAG_IS_QUAL flag to ensure it's a boolean expression
- Uses ExecEvalExprSwitchContext for expression evaluation to handle memory context switching
- Asserts that qualification expressions never return NULL (they must always produce a definite boolean result)
- Converts the resulting Datum to a boolean value using DatumGetBool

This function is fundamental to query execution as it determines which tuples satisfy filtering conditions throughout the execution tree.

## Parameters / Member Variables
- : ExprState pointer containing the compiled expression to evaluate; if NULL, the function returns true (no restrictions)
- : ExprContext providing the execution context including current tuple data, parameter values, and memory contexts

## Dependencies
- Functions called/Symbols referenced:
  - ExecEvalExprSwitchContext
  - DatumGetBool
  - Assert (for debugging builds)
- Called from (representative examples):
  - ExecScan (scan node filtering)
  - ExecNestLoop (join condition evaluation)  
  - ExecHashJoinImpl (hash join condition checking)
  - ExecWithCheckOptions (check constraint validation)
  - TriggerEnabled (trigger condition evaluation)

## Notes and Other Information
- This is an inline function for performance optimization since it's called frequently during query execution
- The EEO_FLAG_IS_QUAL flag verification ensures type safety - only expressions compiled specifically as qualifications can be evaluated
- The function assumes that boolean expressions never return NULL; this is enforced by the expression compiler
- Short-circuiting for NULL state provides an important optimization for queries without WHERE clauses
- Used extensively throughout executor nodes for filtering, join conditions, and constraint checking