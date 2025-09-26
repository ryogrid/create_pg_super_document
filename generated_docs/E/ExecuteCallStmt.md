# ExecuteCallStmt

## Location
[src/backend/commands/functioncmds.c:2188-2364](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L2188-L2364)

## Overview
Executes CALL statements to invoke stored procedures, handling transaction control, parameter evaluation, and result processing in both atomic and non-atomic execution contexts.

## Definition
```c
void ExecuteCallStmt(CallStmt *stmt, ParamListInfo params, bool atomic, DestReceiver *dest)
```

## Detailed Description
This function implements PostgreSQL's CALL statement functionality for executing stored procedures. It handles the complex semantics of procedure calls, particularly around transaction control in non-atomic contexts where procedures can execute COMMIT/ROLLBACK statements. The function creates a CallContext that tracks the atomic/non-atomic execution context, evaluates procedure arguments, performs security checks, and invokes the procedure.

The function manages several critical aspects: permission checking (ACL_EXECUTE), argument evaluation within an appropriate execution context, snapshot management for non-atomic contexts, and result handling for procedures that return records. It enforces restrictions on transaction control based on procedure properties like security definer and proconfig settings.

## Parameters / Member Variables
- `stmt`: CallStmt node containing the parsed CALL statement with function expression and arguments
- `params`: ParamListInfo containing parameter values for prepared statements
- `atomic`: Boolean controlling transaction behavior - false allows transaction commands within the procedure
- `dest`: DestReceiver for sending procedure results back to the client

## Dependencies
- Functions called/Symbols referenced:
  - [object_aclcheck](../o/object_aclcheck.md) (permission verification)
  - [CreateExecutorState](../C/CreateExecutorState.md)/CreateExprContext (execution environment)
  - [ExecPrepareExpr](ExecPrepareExpr.md)/ExecEvalExprSwitchContext (argument evaluation)
  - FunctionCallInvoke (procedure execution)
  - [begin_tup_output_tupdesc](../b/begin_tup_output_tupdesc.md)/end_tup_output (result handling)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)/PushActiveSnapshot (snapshot management)
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- Establishes non-atomic execution context when atomic=false, allowing transaction commands
- Forces atomic=true for security definer procedures and procedures with proconfig
- Handles RECORD-type return values by sending tuples directly to the destination
- Creates CallContext node passed to procedure via fcinfo->context
- Manages snapshots carefully in non-atomic contexts due to potential COMMIT/ROLLBACK
- Includes TOAST pointer safety considerations for procedures that do transaction control
- Part of PostgreSQL's stored procedure infrastructure supporting SQL standard semantics
- Supports nested CALL statements with proper context tracking