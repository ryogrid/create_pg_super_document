# ApplyExecutionData

## Location
[src/backend/replication/logical/worker.c:206-216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L206-L216)

## Overview
ApplyExecutionData is a structure that encapsulates execution state and metadata needed for applying logical replication operations to target relations, including support for partitioned tables.

## Definition

```c
typedef struct ApplyExecutionData
{
	EState	   *estate;			/* executor state, used to track resources */

	LogicalRepRelMapEntry *targetRel;	/* replication target rel */
	ResultRelInfo *targetRelInfo;	/* ResultRelInfo for same */

	/* These fields are used when the target relation is partitioned: */
	ModifyTableState *mtstate;	/* dummy ModifyTable state */
	PartitionTupleRouting *proute;	/* partition routing info */
} ApplyExecutionData;
```
## Detailed Description
ApplyExecutionData serves as a centralized container for execution context during logical replication apply operations. It maintains the executor state, target relation information, and specialized structures for handling partitioned tables. This structure is essential for the logical replication worker to efficiently apply changes from the publisher to the subscriber database while properly managing executor resources and handling complex table structures like partitions.

## Parameters / Member Variables
- `*estate`: Executor state that tracks resources and provides execution context for database operations
- `*targetRel`: Logical replication relation mapping entry containing metadata about the target relation
- `*targetRelInfo`: ResultRelInfo structure providing detailed information about the target relation for execution
- `*mtstate`: ModifyTableState used as a dummy state when dealing with partitioned tables
- `*proute`: PartitionTupleRouting information that handles routing tuples to appropriate partitions
## Dependencies
- Functions called/Symbols referenced:
  - [EState](../E/EState.md)
  - [LogicalRepRelMapEntry](../L/LogicalRepRelMapEntry.md)
  - [ResultRelInfo](../R/ResultRelInfo.md)
  - [ModifyTableState](../M/ModifyTableState.md)
  - [PartitionTupleRouting](../P/PartitionTupleRouting.md)
- Called from (representative examples):
  - [handle_streamed_transaction](../h/handle_streamed_transaction.md)
  - [create_edata_for_relation](../c/create_edata_for_relation.md)
  - [finish_edata](../f/finish_edata.md)
  - [apply_handle_insert](../a/apply_handle_insert.md)
  - [apply_handle_update](../a/apply_handle_update.md)
  - [apply_handle_delete](../a/apply_handle_delete.md)
  - [apply_handle_tuple_routing](../a/apply_handle_tuple_routing.md)

## Notes and Other Information
This structure is primarily used in the logical replication worker process (worker.c) and is crucial for maintaining state consistency during replication operations. The partition-specific fields (mtstate and proute) are only utilized when the target relation is partitioned, allowing the system to efficiently route tuples to the correct partition during apply operations.