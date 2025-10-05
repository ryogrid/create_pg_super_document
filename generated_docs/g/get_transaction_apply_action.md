# get_transaction_apply_action

## Location
[src/backend/replication/logical/worker.c:5126-5166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L5126-L5166)

## Overview
This function determines the appropriate action to take for applying a given transaction in PostgreSQL's logical replication system, considering parallel worker availability and streaming transaction state.

## Definition
static TransApplyAction get_transaction_apply_action(TransactionId xid, ParallelApplyWorkerInfo **winfo)

## Detailed Description
get_transaction_apply_action is a critical decision-making function in PostgreSQL's logical replication apply worker that determines how a transaction should be processed based on the current worker configuration and transaction state. The function evaluates several conditions: whether the current process is a parallel apply worker, whether a parallel worker is available for the transaction, whether the parallel worker is busy (requiring serialization), and whether the transaction is being streamed. Based on these conditions, it returns one of five possible actions: direct parallel application, leader sending to parallel worker, leader partial serialization, leader full serialization, or direct leader application.

## Parameters / Member Variables
- `xid`: The TransactionId of the transaction for which the apply action needs to be determined
- `winfo`: A double pointer to ParallelApplyWorkerInfo that will be set to point to the destination parallel worker info when the leader needs to pass changes to a parallel worker, or NULL otherwise

## Dependencies
- Functions called/Symbols referenced:
  - [am_parallel_apply_worker](../a/am_parallel_apply_worker.md) (check if current process is parallel worker)
  - [pa_find_worker](../p/pa_find_worker.md) (find parallel worker for transaction)
  - TRANS_PARALLEL_APPLY (return value enum)
  - TRANS_LEADER_PARTIAL_SERIALIZE (return value enum) 
  - TRANS_LEADER_SEND_TO_PARALLEL (return value enum)
  - TRANS_LEADER_SERIALIZE (return value enum)
  - TRANS_LEADER_APPLY (return value enum)
  - in_streamed_transaction (global variable)
- Called from (representative examples):
  - [handle_streamed_transaction](../h/handle_streamed_transaction.md) (src/backend/replication/logical/worker.c:568)
  - [apply_handle_stream_prepare](../a/apply_handle_stream_prepare.md) (src/backend/replication/logical/worker.c:1296)
  - [apply_handle_stream_start](../a/apply_handle_stream_start.md) (src/backend/replication/logical/worker.c:1503)
  - [apply_handle_stream_stop](../a/apply_handle_stream_stop.md) (src/backend/replication/logical/worker.c:1638)
  - [apply_handle_stream_abort](../a/apply_handle_stream_abort.md) (src/backend/replication/logical/worker.c:1841)
  - [apply_handle_stream_commit](../a/apply_handle_stream_commit.md) (src/backend/replication/logical/worker.c:2151)

## Notes and Other Information
- This function is static and only used within the worker.c file as part of the internal logical replication infrastructure
- The function implements a decision tree that prioritizes parallel processing when available and falls back to serialization or direct application as needed
- When a parallel worker is busy (serialize_changes flag is set), changes are serialized for later processing rather than blocking
- For streamed transactions without parallel workers, changes are serialized to handle large transactions that might not fit in memory
- The TransApplyAction enum values guide the caller on how to process subsequent transaction operations
- This function is central to PostgreSQL's ability to efficiently handle large-scale logical replication workloads through parallelization and streaming

## Simplified Source

```c
static TransApplyAction get_transaction_apply_action(TransactionId xid, ParallelApplyWorkerInfo **winfo) {
    *winfo = NULL;

    // If we're a parallel worker, just apply directly
    if (am_parallel_apply_worker()) {
        return TRANS_PARALLEL_APPLY;
    }

    // Find if parallel worker exists for this transaction
    *winfo = pa_find_worker(xid);

    if (*winfo && (*winfo)->serialize_changes) {
        // Worker busy, serialize for later processing
        return TRANS_LEADER_PARTIAL_SERIALIZE;
    } else if (*winfo) {
        // Send directly to parallel worker
        return TRANS_LEADER_SEND_TO_PARALLEL;
    }

    // No parallel worker - decide based on streaming state
    if (in_streamed_transaction) {
        return TRANS_LEADER_SERIALIZE;  // Large transaction, serialize
    } else {
        return TRANS_LEADER_APPLY;      // Apply directly
    }
}
```