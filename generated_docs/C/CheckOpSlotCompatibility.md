# CheckOpSlotCompatibility

## Location
[src/backend/executor/execExprInterp.c:2037-2083](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2037-L2083)

## Overview
CheckOpSlotCompatibility is a static function that verifies slot compatibility with EEOP_*_FETCHSOME operations during expression evaluation, ensuring type safety in PostgreSQL's tuple slot handling.

## Definition

```c
static void
CheckOpSlotCompatibility(ExprEvalStep *op, TupleTableSlot *slot)
```
## Detailed Description
This function performs runtime validation to ensure that a TupleTableSlot is compatible with the expected slot type for expression evaluation operations. It operates only in debug builds (USE_ASSERT_CHECKING) and serves as a safety mechanism to catch type mismatches during expression evaluation.

The function handles several compatibility scenarios:
- Allows interchangeable use of BufferHeapTuple and HeapTuple slots for backward compatibility
- Permits virtual slots to be used instead of specific slot types since virtual slots don't require deformation
- Enforces strict type matching for all other cases through assertions

## Parameters / Member Variables
- : Pointer to ExprEvalStep containing the fetch operation details and expected slot type information
- : Pointer to TupleTableSlot that needs to be validated for compatibility

## Dependencies
- Functions called/Symbols referenced:
  - ExprEvalStep (structure access)
  - TTSOpsBufferHeapTuple (slot operations)
  - TTSOpsHeapTuple (slot operations)  
  - TTSOpsVirtual (slot operations)
- Called from (representative examples):
  - EEO_JUMP (macro expansion)
  - [ExecInterpExpr](../E/ExecInterpExpr.md)
  - [ExecJustVarImpl](../E/ExecJustVarImpl.md)
  - [ExecJustAssignVarImpl](../E/ExecJustAssignVarImpl.md)

## Notes and Other Information
- Only active in debug builds (USE_ASSERT_CHECKING), making it a development-time safety check
- Located in src/backend/executor/execExprInterp.c:2037-2083
- Part of PostgreSQL's expression evaluation infrastructure
- Helps maintain type safety during tuple slot operations in the executor