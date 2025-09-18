# ExecInitModifyTable

## Location
[src/backend/executor/nodeModifyTable.c:4422-4822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L4422-L4822)

## Overview
Initializes the execution state for a ModifyTable plan node, setting up all necessary structures for DML operations (INSERT, UPDATE, DELETE, MERGE) including result relations, triggers, constraints, and partitioning.

## Definition


## Detailed Description
This comprehensive initialization function sets up a ModifyTableState structure for executing DML operations. It handles complex scenarios including partitioned tables, foreign tables, inheritance hierarchies, ON CONFLICT clauses, RETURNING projections, WITH CHECK OPTIONS, and MERGE operations.

The function performs several key initialization phases:
1. Creates and initializes the ModifyTableState structure
2. Resolves the root target relation and sets up partition routing if needed
3. Initializes all result relations in the ResultRelInfo array
4. Sets up EPQ (EvalPlanQual) state for concurrent tuple visibility
5. Initializes the subplan that provides input tuples
6. Configures junk attributes for row identification (ctid/wholerow)
7. Sets up RETURNING projections, ON CONFLICT handling, and WITH CHECK OPTIONS
8. Prepares foreign data wrapper interfaces
9. Builds hash tables for efficient result relation lookup when many relations are involved

## Parameters / Member Variables
- : ModifyTable plan node containing the DML operation details and configuration
- : Execution state providing transaction context and global execution information
- : Execution flags controlling behavior (e.g., EXEC_FLAG_EXPLAIN_ONLY for explain-only mode)

## Dependencies
- Functions called/Symbols referenced:
  - [ExecInitResultRelation](ExecInitResultRelation.md)
  - [ExecInitNode](ExecInitNode.md)
  - [EvalPlanQualInit](EvalPlanQualInit.md)
  - [EvalPlanQualSetPlan](EvalPlanQualSetPlan.md)
  - [ExecSetupTransitionCaptureState](ExecSetupTransitionCaptureState.md)
  - [ExecSetupPartitionTupleRouting](ExecSetupPartitionTupleRouting.md)
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md)
  - [ExecBuildProjectionInfo](ExecBuildProjectionInfo.md)
  - [ExecBuildUpdateProjection](ExecBuildUpdateProjection.md)
  - [ExecInitQual](ExecInitQual.md)
  - ExecFindJunkAttributeInTlist
  - [CheckValidResultRel](../C/CheckValidResultRel.md)
  - [ExecInitMerge](ExecInitMerge.md)
- Data structures used:
  - [ModifyTableState](../M/ModifyTableState.md)
  - [ResultRelInfo](../R/ResultRelInfo.md)
  - [OnConflictSetState](../O/OnConflictSetState.md)
  - PlanRowMark
  - [ExecRowMark](ExecRowMark.md)
  - [ExecAuxRowMark](ExecAuxRowMark.md)
- Called from:
  - [ExecInitNode](ExecInitNode.md)

## Notes and Other Information
- Supports all DML operations: INSERT, UPDATE, DELETE, and MERGE
- Handles complex partitioning scenarios with tuple routing for INSERT operations
- Sets up hash tables (mt_resultOidHash) for efficient relation lookup when dealing with many target relations (threshold typically 64 relations)
- Properly initializes foreign data wrapper hooks for foreign table modifications
- Manages row identification through ctid (for regular tables) or wholerow (for foreign tables and views)
- Supports transition table capture for statement-level triggers
- Handles ON CONFLICT DO UPDATE with complete projection setup
- Integrates with EvalPlanQual for proper handling of concurrent updates
- Must be paired with ExecEndModifyTable for proper cleanup
- Critical entry point for all table modification operations in PostgreSQL's executor