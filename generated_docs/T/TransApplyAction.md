# TransApplyAction

## Location
src/backend/replication/logical/worker.c: 275 - 335

## Overview
TransApplyAction is an enumeration that defines different actions for processing transaction changes in PostgreSQL's logical replication system, determining how changes are handled by leader workers, parallel workers, and during streaming transactions.

## Definition


## Detailed Description
The TransApplyAction enum specifies how transaction changes should be processed in PostgreSQL's logical replication worker system. This enum is crucial for coordinating between leader apply workers, parallel apply workers, and table sync workers, especially when handling streaming transactions that may be too large to apply directly.

The enum supports both non-streaming and streaming transaction workflows, with different actions for serializing changes to temporary files, sending changes to parallel workers, or applying changes directly. The choice of action depends on factors such as transaction size, worker type, timeout conditions, and parallel processing capabilities.

## Parameters / Member Variables
- : Used by leader apply workers or table sync workers to either directly apply transaction changes or read from temporary files (for streaming transactions) and then apply them
- : Used by leader apply workers or table sync workers to write changes to temporary files, deferring application until the final commit arrives
- : Used by leader apply workers when changes need to be sent to a parallel apply worker for processing
- : Used by leader apply workers when some changes have been sent to parallel workers but remaining changes must be serialized to files due to timeout conditions during data transmission
- : Used by parallel apply workers to directly apply transaction changes received from the leader worker

## Dependencies
- Functions called/Symbols referenced:
  - ApplyErrorCallbackArg
  - [WalReceiverConn](../W/WalReceiverConn.md)
  - [Subscription](../S/Subscription.md)
- Called from (representative examples):
  - [handle_streamed_transaction](../h/handle_streamed_transaction.md) (src/backend/replication/logical/worker.c:565)
  - [apply_handle_stream_prepare](../a/apply_handle_stream_prepare.md) (src/backend/replication/logical/worker.c:1277)
  - [apply_handle_stream_start](../a/apply_handle_stream_start.md) (src/backend/replication/logical/worker.c:1473)
  - [apply_handle_stream_stop](../a/apply_handle_stream_stop.md) (src/backend/replication/logical/worker.c:1631)
  - [apply_handle_stream_abort](../a/apply_handle_stream_abort.md) (src/backend/replication/logical/worker.c:1820)
  - [apply_handle_stream_commit](../a/apply_handle_stream_commit.md) (src/backend/replication/logical/worker.c:2138)
  - [set_apply_error_context_origin](../s/set_apply_error_context_origin.md) (src/backend/replication/logical/worker.c:5125)

## Notes and Other Information
- This enum is specifically designed to handle the complexity of streaming transactions in logical replication
- TRANS_LEADER_PARTIAL_SERIALIZE was introduced as a distinct action from TRANS_LEADER_SERIALIZE because it requires additional handling of STREAM_XXX messages and coordination with parallel workers
- The choice of action affects memory usage, performance, and the coordination between different types of replication workers
- Used primarily in the context of PostgreSQL's logical replication worker processes (worker.c:275-335)