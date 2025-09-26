# ExecInitForeignScan

## Location
[src/backend/executor/nodeForeignscan.c:142-296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeForeignscan.c#L142-L296)

## Overview
ExecInitForeignScan initializes a ForeignScanState node for executing foreign scan operations, setting up all necessary execution context, tuple descriptors, and FDW-specific state.

## Definition

```c
structure
	 */
	scanstate = makeNode(ForeignScanState);
```
## Detailed Description
ExecInitForeignScan is a comprehensive initialization function that prepares a foreign scan node for execution within PostgreSQL's executor framework. This function handles the complex setup required for interfacing with Foreign Data Wrappers (FDWs) while integrating seamlessly with PostgreSQL's execution infrastructure.

The initialization process encompasses several critical areas:

**Execution State Setup**: Creates and configures the ForeignScanState structure, establishes expression contexts, and sets up the execution pipeline by assigning ExecForeignScan as the execution function.

**Relation and FDW Handling**: Manages both regular foreign table scans (with scanrelid > 0) and foreign joins or subqueries (scanrelid = 0). For regular scans, it opens the scan relation and retrieves FDW routines through the relation cache. For complex cases, it obtains FDW routines directly via server ID.

**Tuple Descriptor Management**: Determines the appropriate scan tuple type based on whether the FDW provides a custom targetlist. If provided, it uses ExecTypeFromTL to create the descriptor; otherwise, it copies the base relation's tuple descriptor. The function carefully handles variable numbering (INDEX_VAR vs scanrelid) to ensure proper projection setup.

**Expression Initialization**: Sets up both regular qualifications and FDW-specific recheck qualifications, preparing them for execution. Also initializes result types and projection information with proper variable number mapping.

**Asynchronous Execution Support**: Configures async_capable flag based on the plan's capabilities and current execution context, coordinating with other executor nodes like Append.

**Modification Target Setup**: For direct modification operations, establishes connections to the target ResultRelInfo, ensuring proper coordination with ModifyTable nodes.

**FDW Callback Invocation**: Finally delegates to the appropriate FDW initialization routine (BeginForeignScan or BeginDirectModify) based on the operation type, with special handling for EvalPlanQual scenarios.

## Parameters / Member Variables
- : ForeignScan plan node containing scan parameters, target lists, qualifications, and FDW-specific configuration
- : EState execution state providing transaction context, memory management, and executor-wide state information
- : Integer flags controlling initialization behavior, with assertions preventing unsupported backward scanning and mark/restore operations

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [ExecAssignExprContext](ExecAssignExprContext.md)
  - [ExecOpenScanRelation](ExecOpenScanRelation.md)
  - [GetFdwRoutineForRelation](../G/GetFdwRoutineForRelation.md)
  - [GetFdwRoutineByServerId](../G/GetFdwRoutineByServerId.md)
  - [ExecTypeFromTL](ExecTypeFromTL.md)
  - [ExecInitScanTupleSlot](ExecInitScanTupleSlot.md)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md)
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md)
  - [ExecAssignScanProjectionInfoWithVarno](ExecAssignScanProjectionInfoWithVarno.md)
  - [ExecInitQual](ExecInitQual.md)
  - [ExecInitNode](ExecInitNode.md)
  - BeginForeignScan (via fdwroutine)
  - BeginDirectModify (via fdwroutine)
- Called from:
  - [ExecInitNode](ExecInitNode.md)

## Notes and Other Information
- The function explicitly rejects EXEC_FLAG_BACKWARD and EXEC_FLAG_MARK flags as foreign scans don't support these operations
- Tuple descriptor copying for base relations deliberately doesn't trust FDWs to maintain NOT NULL constraints
- Direct modification operations are special-cased to avoid initialization during EvalPlanQual processing
- Async capability is synchronized with ExecInitAppend() behavior for consistency
- The scanopsfixed flag is set to false since FDW return formats are unpredictable
- [ResultRelInfo](../R/ResultRelInfo.md) lookup is skipped during EvalPlanQual to avoid initialization issues
- Outer plan initialization supports complex query structures with foreign scans as inner nodes