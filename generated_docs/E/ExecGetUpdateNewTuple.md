# ExecGetUpdateNewTuple

## Location
[src/backend/executor/nodeModifyTable.c:741-778](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L741-L778)

## Overview
Prepares a new tuple for UPDATE operations by combining changed column values from the subplan with unchanged columns from the old tuple.

## Definition

```c
TupleTableSlot *
ExecGetUpdateNewTuple(ResultRelInfo *relinfo,
					  TupleTableSlot *planSlot,
					  TupleTableSlot *oldSlot)
```
## Detailed Description
This function is the core mechanism for constructing updated tuples in PostgreSQL's UPDATE operations. It performs the essential task of merging:
- New values for modified columns from the subplan's output (planSlot)
- Existing values for unmodified columns from the current tuple (oldSlot)

The function leverages the projection system to seamlessly combine these two data sources into a complete, updated tuple. The projection was previously set up by ExecInitUpdateProjection() and contains the logic for:
1. Extracting new values for updated columns from the outer tuple (planSlot)
2. Preserving existing values for non-updated columns from the scan tuple (oldSlot)  
3. Filtering out any junk columns from the subplan output
4. Ensuring the result matches the target table's tuple format

Unlike the INSERT case, UPDATE operations always require projection due to the complex merging requirements and presence of junk attributes.

## Parameters / Member Variables
- `*relinfo`: Result relation information containing the initialized projection (ri_projectNew)
- `*planSlot`: Tuple table slot from the UPDATE subplan containing new values for changed columns
- `*oldSlot`: Tuple table slot containing the existing tuple with current column values
## Dependencies
- Functions called/Symbols referenced:
  - TTS_EMPTY (macro)
  - [ExecProject](ExecProject.md)
- Called from (representative examples):
  - [ExecBRUpdateTriggersNew](ExecBRUpdateTriggersNew.md)
  - [ExecCrossPartitionUpdate](ExecCrossPartitionUpdate.md)
  - [ExecUpdate](ExecUpdate.md)
  - [ExecModifyTable](ExecModifyTable.md)

## Notes and Other Information
- Unlike ExecGetInsertNewTuple, this is a public function (not static) used across multiple modules
- Includes defensive assertions to validate that projection info is properly initialized and both input slots contain valid data
- The projection system handles the complex logic of merging old and new column values
- Always returns ri_newTupleSlot through the projection, ensuring proper slot type for the target relation
- Critical component of PostgreSQL's UPDATE execution pipeline
- The expression context setup maps planSlot to ecxt_outertuple and oldSlot to ecxt_scantuple for projection evaluation

## Simplified Source

```c
// Simplified version of ExecGetUpdateNewTuple
TupleTableSlot *ExecGetUpdateNewTuple(ResultRelInfo *relinfo,
                                      TupleTableSlot *planSlot,
                                      TupleTableSlot *oldSlot) {
    // Get the projection info for merging old and new values
    ProjectionInfo *newProj = relinfo->ri_projectNew;

    // Validate inputs (safety checks)
    Assert(relinfo->ri_projectNewInfoValid);
    Assert(planSlot != NULL && !TTS_EMPTY(planSlot));
    Assert(oldSlot != NULL && !TTS_EMPTY(oldSlot));

    // Setup expression context with both tuple sources
    ExprContext *econtext = newProj->pi_exprContext;
    econtext->ecxt_outertuple = planSlot;   // New values from subplan
    econtext->ecxt_scantuple = oldSlot;     // Original values

    // Project to create the merged tuple
    return ExecProject(newProj);
}
```

Key simplifications made:
- Added clear comments explaining the tuple merging process
- Simplified variable access for readability
- Emphasized the dual input nature (new values + old values)
- Preserved all essential safety checks and logic