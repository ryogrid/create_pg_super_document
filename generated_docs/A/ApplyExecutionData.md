# ApplyExecutionData

## Location
src/backend/replication/logical/worker.c: 206 - 216

## Overview
ApplyExecutionData is a structure that encapsulates execution state and metadata needed for applying logical replication operations to target relations, including support for partitioned tables.

## Definition


## Detailed Description
ApplyExecutionData serves as a centralized container for execution context during logical replication apply operations. It maintains the executor state, target relation information, and specialized structures for handling partitioned tables. This structure is essential for the logical replication worker to efficiently apply changes from the publisher to the subscriber database while properly managing executor resources and handling complex table structures like partitions.

## Parameters / Member Variables
- : Executor state that tracks resources and provides execution context for database operations
- : Logical replication relation mapping entry containing metadata about the target relation
- : ResultRelInfo structure providing detailed information about the target relation for execution
- : ModifyTableState used as a dummy state when dealing with partitioned tables
- : PartitionTupleRouting information that handles routing tuples to appropriate partitions

## Dependencies
- Functions called/Symbols referenced:
  - EState
  - LogicalRepRelMapEntry
  - ResultRelInfo
  - ModifyTableState
  - PartitionTupleRouting
- Called from (representative examples):
  - handle_streamed_transaction
  - create_edata_for_relation
  - finish_edata
  - apply_handle_insert
  - apply_handle_update
  - apply_handle_delete
  - apply_handle_tuple_routing

## Notes and Other Information
This structure is primarily used in the logical replication worker process (worker.c) and is crucial for maintaining state consistency during replication operations. The partition-specific fields (mtstate and proute) are only utilized when the target relation is partitioned, allowing the system to efficiently route tuples to the correct partition during apply operations.