# SubscriptExecSteps

## Location
[src/include/executor/execExpr.h:758-765](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/execExpr.h#L758-L765)

## Overview
SubscriptExecSteps defines a set of function pointers for executing container subscripting operations, providing a pluggable interface for different container types like arrays and JSONB.

## Definition

```c
typedef struct SubscriptExecSteps
{
	/* See nodes/subscripting.h for more detail about these */
	ExecEvalBoolSubroutine sbs_check_subscripts;	/* process subscripts */
	ExecEvalSubroutine sbs_fetch;	/* fetch an element */
	ExecEvalSubroutine sbs_assign;	/* assign to an element */
	ExecEvalSubroutine sbs_fetch_old;	/* fetch old value for assignment */
} SubscriptExecSteps;
```
## Detailed Description
SubscriptExecSteps provides a function pointer interface that allows different container types (arrays, JSONB, etc.) to implement their own subscripting behavior within PostgreSQL's expression evaluation framework. This design enables type-specific optimizations while maintaining a consistent interface for the expression evaluator.

Each container type implements these four essential operations: checking subscript validity, fetching values, assigning new values, and retrieving old values before assignment. The structure acts as a virtual function table (vtable) allowing polymorphic behavior for subscripting operations.

The functions are designed to work with the SubscriptingRefState structure and integrate seamlessly with PostgreSQL's ExprEvalStep-based expression evaluation system.

## Parameters / Member Variables
- `sbs_check_subscripts`: Function pointer to validate and process subscript expressions, returns boolean success status
- `sbs_fetch`: Function pointer to retrieve a value from the container using computed subscripts
- `sbs_assign`: Function pointer to assign a new value to the container at the specified subscripts
- `sbs_fetch_old`: Function pointer to retrieve the current value before assignment (used for nested assignments and certain update operations)
## Dependencies
- Functions called/Symbols referenced:
  - ExecEvalBoolSubroutine (function pointer type for boolean-returning evaluation routines)
  - ExecEvalSubroutine (function pointer type for standard evaluation routines)
- Called from (representative examples):
  - [ExecInitSubscriptingRef](../E/ExecInitSubscriptingRef.md) (expression initialization, sets up function pointers)
  - [array_exec_setup](../a/array_exec_setup.md) (array-specific implementation setup)
  - [jsonb_exec_setup](../j/jsonb_exec_setup.md) (JSONB-specific implementation setup)

## Notes and Other Information
- Provides a polymorphic interface for container subscripting operations
- Different container types (arrays, JSONB) provide their own implementations of these function pointers
- Enables type-specific optimizations while maintaining interface consistency
- Works closely with SubscriptingRefState to manage operation state
- Part of PostgreSQL's extensible subscripting system introduced to support different container types
- The boolean return from sbs_check_subscripts allows for early termination on invalid subscripts
- Used in expression evaluation steps of type EEOP_SBSREF_SUBSCRIPTS, EEOP_SBSREF_OLD, EEOP_SBSREF_ASSIGN, and EEOP_SBSREF_FETCH