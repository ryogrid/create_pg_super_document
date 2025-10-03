# InitResultRelInfo

## Location
[src/backend/executor/execMain.c:1196-1294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L1196-L1294)

## Overview
Initializes a ResultRelInfo structure for a result relation, setting up all necessary fields for data modification operations including triggers, foreign data wrappers, and partitioning information.

## Definition

```c
void
InitResultRelInfo(ResultRelInfo *resultRelInfo,
				  Relation resultRelationDesc,
				  Index resultRelationIndex,
				  ResultRelInfo *partition_root_rri,
				  int instrument_options)
```
## Detailed Description
InitResultRelInfo performs comprehensive initialization of a ResultRelInfo structure, which contains all the metadata and state information needed for data modification operations on a target relation. The function sets up trigger-related structures, foreign data wrapper routines for foreign tables, and partitioning-related fields. It creates deep copies of trigger descriptors to avoid dependency on relcache changes and allocates arrays for trigger functions and expressions based on instrumentation requirements.

## Parameters / Member Variables
- `*resultRelInfo`: The ResultRelInfo structure to initialize (output parameter)
- `resultRelationDesc`: The target relation descriptor containing metadata
- `resultRelationIndex`: Index of this relation in the range table
- `*partition_root_rri`: Root partition's ResultRelInfo for partitioned relations (NULL for non-partitioned)
- `instrument_options`: Bitmask for enabling performance instrumentation on triggers
## Dependencies
- Functions called/Symbols referenced:
  - MemSet
  - [IsInplaceUpdateRelation](IsInplaceUpdateRelation.md)
  - [CopyTriggerDesc](../C/CopyTriggerDesc.md)
  - [InstrAlloc](InstrAlloc.md)
  - [GetFdwRoutineForRelation](../G/GetFdwRoutineForRelation.md)
  - Various MERGE and RELKIND constants
- Called from (representative examples):
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md)
  - [ExecGetTriggerResultRel](../E/ExecGetTriggerResultRel.md)
  - [ExecGetAncestorResultRels](../E/ExecGetAncestorResultRels.md)
  - [ExecInitPartitionInfo](../E/ExecInitPartitionInfo.md)
  - [ExecInitPartitionDispatchInfo](../E/ExecInitPartitionDispatchInfo.md)
  - [ExecInitResultRelation](../E/ExecInitResultRelation.md)

## Notes and Other Information
- Prior to PostgreSQL 9.1, this function also handled relation kind validation and index opening
- Creates deep copies of trigger descriptors to maintain independence from relcache changes
- Initializes extensive arrays for trigger functions, expressions, and instrumentation when needed
- Sets up FDW routines specifically for foreign tables using GetFdwRoutineForRelation
- Many fields are initialized to NULL/NIL and set later during execution as needed
- Handles partition hierarchy relationships through ri_RootResultRelInfo field
- Essential part of the executor's initialization phase for any data modification operation

## Simplified Source

```c
void
InitResultRelInfo(ResultRelInfo *resultRelInfo,
                  Relation resultRelationDesc,
                  Index resultRelationIndex,
                  ResultRelInfo *partition_root_rri,
                  int instrument_options)
{
    // Initialize basic structure fields
    MemSet(resultRelInfo, 0, sizeof(ResultRelInfo));
    resultRelInfo->type = T_ResultRelInfo;
    resultRelInfo->ri_RangeTableIndex = resultRelationIndex;
    resultRelInfo->ri_RelationDesc = resultRelationDesc;

    // Set up index and lock information
    resultRelInfo->ri_NumIndices = 0;
    resultRelInfo->ri_needLockTagTuple = IsInplaceUpdateRelation(resultRelationDesc);

    // Copy trigger descriptor to avoid relcache dependencies
    resultRelInfo->ri_TrigDesc = CopyTriggerDesc(resultRelationDesc->trigdesc);

    if (resultRelInfo->ri_TrigDesc) {
        int n = resultRelInfo->ri_TrigDesc->numtriggers;

        // Allocate trigger function and expression arrays
        resultRelInfo->ri_TrigFunctions = palloc0(n * sizeof(FmgrInfo));
        resultRelInfo->ri_TrigWhenExprs = palloc0(n * sizeof(ExprState *));

        if (instrument_options)
            resultRelInfo->ri_TrigInstrument = InstrAlloc(n, instrument_options, false);
    }

    // Set up FDW routine for foreign tables
    if (resultRelationDesc->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
        resultRelInfo->ri_FdwRoutine = GetFdwRoutineForRelation(resultRelationDesc, true);

    // Initialize remaining fields to NULL/NIL (will be set later as needed)
    resultRelInfo->ri_RootResultRelInfo = partition_root_rri;
    // ... other fields initialized to defaults
}
```