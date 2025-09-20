# ExecUpdatePrepareSlot

## Location
[src/backend/executor/nodeModifyTable.c:1967-2001](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L1967-L2001)

## Overview
A subroutine for ExecUpdateAct that applies final modifications to a tuple slot before performing the update, including setting table OID and computing stored generated columns.

## Definition

```c
static void
ExecUpdatePrepareSlot(ResultRelInfo *resultRelInfo,
					  TupleTableSlot *slot,
					  EState *estate)
```
## Detailed Description
ExecUpdatePrepareSlot performs the final preparation steps on a tuple slot before the actual update operation is executed. This function ensures that the tuple slot contains all necessary metadata and computed values required for a successful update.

The function handles two critical preparation tasks:
1. **Table OID Initialization**: Sets the tts_tableOid field in the tuple slot to the relation's OID, which may be referenced by constraints and GENERATED expressions
2. **Stored Generated Column Computation**: Calculates values for any stored generated columns based on the current tuple data and UPDATE context

This preparation is essential because constraints and generated column expressions may need to reference the tableoid system column, and generated columns must be computed with the latest values before the tuple is stored.

## Parameters / Member Variables
- : Information about the result relation being updated, containing relation descriptor and metadata
- : TupleTableSlot containing the tuple data to be updated, which will be modified in-place
- : EState containing the execution state and context for the current query

## Dependencies
- Functions called/Symbols referenced:
  - [ExecComputeStoredGenerated](ExecComputeStoredGenerated.md) (computes stored generated column values)
  - CMD_UPDATE (command type constant for UPDATE operations)
  - RelationGetRelid (extracts relation OID from relation descriptor)
- Called from:
  - [ExecUpdateAct](ExecUpdateAct.md) (during the main update execution path)
  - [ExecUpdate](ExecUpdate.md) (for foreign table updates)

## Notes and Other Information
- This is a static function, only accessible within nodeModifyTable.c
- The function modifies the slot in-place, updating its tts_tableOid field
- Generated column computation only occurs if the table has stored generated columns with constraints
- Split out as a separate function to allow reuse in both regular table and foreign table update paths
- The tableOid setting is crucial for expressions that reference the tableoid system column
- Part of PostgreSQL's UPDATE execution pipeline, specifically handling tuple preparation