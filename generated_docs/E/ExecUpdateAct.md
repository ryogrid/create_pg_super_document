# ExecUpdateAct

## Location
[src/backend/executor/nodeModifyTable.c:2002-2152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeModifyTable.c#L2002-L2152)

## Overview
ExecUpdateAct is a subroutine for ExecUpdate that performs the actual tuple update operation on a plain table, handling partition constraint checks and cross-partition tuple migration when necessary.

## Definition

```c
static TM_Result
ExecUpdateAct(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
			  ItemPointer tupleid, HeapTuple oldtuple, TupleTableSlot *slot,
			  bool canSetTag, UpdateContext *updateCxt)
```
## Detailed Description
ExecUpdateAct is responsible for the core logic of updating a tuple in a PostgreSQL table. The function handles several critical aspects of the update operation:

1. **Generated Column Processing**: Fills in GENERATED columns using ExecUpdatePrepareSlot
2. **Partition Constraint Validation**: Checks if the updated tuple still satisfies the partition constraint
3. **Row-Level Security**: Validates RLS UPDATE WITH CHECK policies when partition constraints pass
4. **Cross-Partition Updates**: When partition constraints fail, attempts to move the tuple to the correct partition via ExecCrossPartitionUpdate
5. **Constraint Validation**: Ensures the updated tuple satisfies all table constraints
6. **Physical Update**: Performs the actual heap tuple update using table_tuple_update

The function uses a retry mechanism (via the  label) to handle cases where cross-partition updates require recomputation of GENERATED values and constraint rechecking for the destination partition.

## Parameters / Member Variables
- : ModifyTableContext containing execution state and metadata
- : ResultRelInfo for the target relation being updated
- : ItemPointer identifying the specific tuple to update
- : HeapTuple containing the original tuple data
- : TupleTableSlot containing the new tuple values
- : Boolean indicating whether command tags can be set
- : UpdateContext for tracking update-specific state and results

## Dependencies
- Functions called/Symbols referenced:
  - [ExecUpdatePrepareSlot](ExecUpdatePrepareSlot.md)
  - ExecMaterializeSlot
  - [ExecPartitionCheck](ExecPartitionCheck.md)
  - [ExecWithCheckOptions](ExecWithCheckOptions.md)
  - [ExecCrossPartitionUpdate](ExecCrossPartitionUpdate.md)
  - [ExecCrossPartitionUpdateForeignKey](ExecCrossPartitionUpdateForeignKey.md)
  - [ExecConstraints](ExecConstraints.md)
  - table_tuple_update
- Called from (representative examples):
  - [ExecUpdate](ExecUpdate.md) (src/backend/executor/nodeModifyTable.c:2358)
  - [ExecMergeMatched](ExecMergeMatched.md) (src/backend/executor/nodeModifyTable.c:3048)

## Notes and Other Information
- The function is static and only used within nodeModifyTable.c
- Handles both regular updates and cross-partition updates transparently
- The retry loop (lreplace) is specifically designed for cross-partition scenarios where GENERATED values may differ between partitions
- For MERGE operations, cross-partition update retries are handled differently and delegated back to the MERGE logic
- The function integrates with PostgreSQL's tuple visibility and concurrency control mechanisms through table_tuple_update
- Foreign key constraint checking for cross-partition updates is handled via ExecCrossPartitionUpdateForeignKey