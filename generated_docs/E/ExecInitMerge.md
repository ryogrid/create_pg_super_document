# ExecInitMerge

## Location
[src/backend/executor/nodeModifyTable.c:3484-3761](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L3484-L3761)

## Overview
Initializes state and data structures required for executing MERGE statements, including action states, join conditions, projections, and tuple slots.

## Definition

```c
struct and the root
					 * table's "new" tuple slot for that, if not already done.
					 * The projection we prepare, for all relations, uses the
					 * root relation descriptor, and targets the plan's root
					 * slot.  (This is consistent with the fact that we
					 * checked the plan output to match the root relation,
					 * above.)
					 */
					if (rootRelInfo->ri_RelationDesc->rd_rel->relkind ==
						RELKIND_PARTITIONED_TABLE)
					{
						if (mtstate->mt_partition_tuple_routing == NULL)
						{
							/*
							 * Initialize planstate for routing if not already
							 * done.
							 *
							 * Note that the slot is managed as a standalone
							 * slot belonging to ModifyTableState, so we pass
							 * NULL for the 2nd argument.
							 */
							mtstate->mt_root_tuple_slot =
								table_slot_create(rootRelInfo->ri_RelationDesc,
												  NULL);
							mtstate->mt_partition_tuple_routing =
								ExecSetupPartitionTupleRouting(estate,
															   rootRelInfo->ri_RelationDesc);
						}
						tgtslot = mtstate->mt_root_tuple_slot;
						tgtdesc = RelationGetDescr(rootRelInfo->ri_RelationDesc);
					}
					else
					{
						/*
						 * If the MERGE targets an inherited table, we insert
						 * into the root table, so we must initialize its
						 * "new" tuple slot, if not already done, and use its
						 * relation descriptor for the projection.
						 *
						 * For non-inherited tables, rootRelInfo and
						 * resultRelInfo are the same, and the "new" tuple
						 * slot will already have been initialized.
						 */
						if (rootRelInfo->ri_newTupleSlot == NULL)
							rootRelInfo->ri_newTupleSlot =
								table_slot_create(rootRelInfo->ri_RelationDesc,
												  &estate->es_tupleTable);

						tgtslot = rootRelInfo->ri_newTupleSlot;
						tgtdesc = RelationGetDescr(rootRelInfo->ri_RelationDesc);
					}

					action_state->mas_proj =
						ExecBuildProjectionInfo(action->targetList, econtext,
												tgtslot,
												&mtstate->ps,
												tgtdesc);
```
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
- `NULL)`: ModifyTableState containing the execution state and plan information for the MERGE operation
- `rootRelInfo->ri_RelationDesc)`: EState providing the execution environment and context

## Dependencies
- Functions called/Symbols referenced:
  - [ExecInitMergeTupleSlots](ExecInitMergeTupleSlots.md)
  - [ExecInitQual](ExecInitQual.md)
  - ExecAssignExprContext
  - [ExecCheckPlanOutput](ExecCheckPlanOutput.md)
  - [ExecBuildProjectionInfo](ExecBuildProjectionInfo.md)
  - [ExecBuildUpdateProjection](ExecBuildUpdateProjection.md)
  - [ExecSetupPartitionTupleRouting](ExecSetupPartitionTupleRouting.md)
  - [table_slot_create](../t/table_slot_create.md)
  - [build_attrmap_by_name](../b/build_attrmap_by_name.md)
  - [map_variable_attnos](../m/map_variable_attnos.md)
- Called from (representative examples):
  - [ExecInitModifyTable](ExecInitModifyTable.md)

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