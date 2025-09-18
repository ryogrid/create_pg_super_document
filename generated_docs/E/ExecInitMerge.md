# ExecInitMerge

## Location
src/backend/executor/nodeModifyTable.c: 3484 - 3761

## Overview
Initializes state and data structures required for executing MERGE statements, including action states, join conditions, projections, and tuple slots.

## Definition


## Detailed Description
ExecInitMerge performs comprehensive initialization for MERGE statement execution by setting up all necessary runtime structures:

1. **Action State Initialization**: Creates MergeActionState objects for each MERGE action (INSERT, UPDATE, DELETE, DO NOTHING) and organizes them into lists by match type (MATCHED, NOT MATCHED BY SOURCE, NOT MATCHED BY TARGET)

2. **Join Condition Setup**: Initializes join condition expressions that determine whether source and target tuples match

3. **Projection Initialization**: Sets up projection info for different action types:
   - INSERT actions: Projects new tuples for insertion, handling partitioned tables specially
   - UPDATE actions: Creates update projections with column mapping
   - DELETE actions: Tracks subcommand flags

4. **Tuple Slot Management**: Initializes tuple slots for merge operations via ExecInitMergeTupleSlots

5. **Partitioned Table Support**: Sets up partition tuple routing for INSERT operations on partitioned tables

6. **Constraint Handling**: Initializes WITH CHECK OPTION constraints and RETURNING projections for inherited tables

The function handles complex scenarios involving inheritance and partitioning, ensuring that INSERT actions are properly routed through root relations while maintaining correct attribute mappings.

## Parameters / Member Variables
- : ModifyTableState containing the execution state and plan information for the MERGE operation
- : EState providing the execution environment and context

## Dependencies
- Functions called/Symbols referenced:
  - ExecInitMergeTupleSlots
  - ExecInitQual
  - ExecAssignExprContext
  - ExecCheckPlanOutput
  - ExecBuildProjectionInfo
  - ExecBuildUpdateProjection
  - ExecSetupPartitionTupleRouting
  - table_slot_create
  - build_attrmap_by_name
  - map_variable_attnos
- Called from (representative examples):
  - ExecInitModifyTable

## Notes and Other Information
- Handles three types of MERGE actions: MATCHED, NOT MATCHED BY SOURCE, and NOT MATCHED BY TARGET
- Sets mt_merge_subcommands bitmask to track which types of operations (INSERT/UPDATE/DELETE) are present
- Special handling for partitioned tables where INSERT actions must be routed through the root relation
- Manages complex attribute mapping for inherited tables to ensure correct column references
- Initializes Row Level Security (RLS) WITH CHECK OPTION expressions when present  
- Sets up RETURNING clause projections for INSERT actions on inherited tables
- Uses the first relation as a reference when building constraints and projections for root relations
- Early return if no merge actions are present (node->mergeActionLists == NIL)
- Similar initialization logic appears in ExecInitPartitionInfo() for partition-specific setup