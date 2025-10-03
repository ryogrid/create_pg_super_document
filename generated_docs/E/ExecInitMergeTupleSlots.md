# ExecInitMergeTupleSlots

## Location
[src/backend/executor/nodeModifyTable.c:3762-3781](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L3762-L3781)

## Overview
Initializes the tuple slots in a ResultRelInfo structure specifically for MERGE operations, creating both old and new tuple slots for the target relation.

## Definition

```c
void
ExecInitMergeTupleSlots(ModifyTableState *mtstate,
						ResultRelInfo *resultRelInfo)
```
## Detailed Description
ExecInitMergeTupleSlots is a utility function that sets up the essential tuple storage slots required for MERGE operations on a specific result relation. The function performs the following operations:

1. **Old Tuple Slot Creation**: Creates ri_oldTupleSlot to store existing target tuples that are fetched during MATCHED and NOT MATCHED BY SOURCE operations
2. **New Tuple Slot Creation**: Creates ri_newTupleSlot to store new or updated tuple data during INSERT and UPDATE operations
3. **Validation Flag Setting**: Marks ri_projectNewInfoValid as true to indicate that the tuple slots are properly initialized

The tuple slots are created using the relation's descriptor and are registered with the estate's tuple table for proper memory management. This function is called during initialization to ensure that MERGE operations have the necessary storage infrastructure.

Note that while this function initializes the tuple slots, it does not initialize the projection info structures themselves - those are set up separately during action-specific initialization.

## Parameters / Member Variables
- `*mtstate`: ModifyTableState containing the executor state and plan information
- `*resultRelInfo`: ResultRelInfo structure for the target relation that will receive the tuple slots
## Dependencies
- Functions called/Symbols referenced:
  - [table_slot_create](../t/table_slot_create.md)
  - [ModifyTableState](../M/ModifyTableState.md)
- Called from (representative examples):
  - [ExecInitMerge](ExecInitMerge.md)
  - [ExecInitPartitionInfo](ExecInitPartitionInfo.md)

## Notes and Other Information
- The function includes an assertion to ensure it's not called multiple times on the same ResultRelInfo (ri_projectNewInfoValid must be false)
- Both tuple slots are created using the same relation descriptor but serve different purposes during MERGE execution
- The ri_oldTupleSlot is used to hold target tuples that are fetched from storage for MATCHED operations
- The ri_newTupleSlot is used to hold projected tuples for INSERT and UPDATE operations
- Memory management is handled automatically through registration with estate->es_tupleTable
- This function is also used by partition initialization code, making it a shared utility for MERGE operations across different relation types
- The ri_projectNewInfoValid flag prevents duplicate initialization and indicates that the ResultRelInfo is ready for MERGE projections

## Simplified Source

```c
void
ExecInitMergeTupleSlots(ModifyTableState *mtstate,
                       ResultRelInfo *resultRelInfo)
{
    EState *estate = mtstate->ps.state;

    // Ensure this hasn't been called before
    Assert(!resultRelInfo->ri_projectNewInfoValid);

    // Create slot for existing target tuples (MATCHED operations)
    resultRelInfo->ri_oldTupleSlot =
        table_slot_create(resultRelInfo->ri_RelationDesc,
                         &estate->es_tupleTable);

    // Create slot for new/updated tuples (INSERT/UPDATE operations)
    resultRelInfo->ri_newTupleSlot =
        table_slot_create(resultRelInfo->ri_RelationDesc,
                         &estate->es_tupleTable);

    // Mark tuple slots as initialized
    resultRelInfo->ri_projectNewInfoValid = true;
}
```