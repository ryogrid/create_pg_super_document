# ShutdownSetExpr

## Location
[src/backend/executor/execSRF.c:810-833](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execSRF.c#L810-L833)

## Overview
ShutdownSetExpr is a callback function responsible for properly cleaning up a SetExprState structure when it needs to be shut down before completion, ensuring all allocated resources are properly released.

## Definition
```c
static void ShutdownSetExpr(Datum arg)
```

## Detailed Description
This function serves as a cleanup callback for SetExprState structures in PostgreSQL's executor framework. It is specifically designed to handle cases where a set-returning function (SRF) evaluation needs to be terminated before it has run to completion. The function ensures that all resources associated with the SetExprState are properly released, including tuple slots, tuplestores, and argument state.

The function is typically registered as a callback using PostgreSQL's expression context callback mechanism, allowing the executor to automatically clean up resources when the expression context is destroyed or reset.

## Parameters / Member Variables
- `arg`: A Datum containing a pointer to the SetExprState structure that needs to be shut down

## Dependencies
- Functions called/Symbols referenced:
  - castNode (for type-safe casting to SetExprState)
  - ExecClearTuple (to clear the function result slot)
  - [tuplestore_end](../t/tuplestore_end.md) (to release the tuplestore)
  - [DatumGetPointer](../D/DatumGetPointer.md) (to extract pointer from Datum)
- Called from (representative examples):
  - [ExecMakeFunctionResultSet](../E/ExecMakeFunctionResultSet.md) (registers this as a callback)
  - [ExecPrepareTuplestoreResult](../E/ExecPrepareTuplestoreResult.md) (registers this as a callback)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the execSRF.c compilation unit
- The function sets shutdown_reg to false to indicate that the callback has been executed
- It handles null pointers gracefully by checking if funcResultSlot and funcResultStore exist before operating on them
- The setArgsValid flag is set to false to invalidate any cached set argument state
- This callback mechanism is crucial for preventing memory leaks in PostgreSQL's expression evaluation system