# pa_unlock_stream

## Location
[src/backend/replication/logical/applyparallelworker.c:1547-1572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L1547-L1572)

## Overview
Unlocks the parallel apply stream lock for a specific transaction, allowing other parallel apply workers to process the stream.

## Definition

```c
void
pa_unlock_stream(TransactionId xid, LOCKMODE lockmode)
```
## Detailed Description
This function is part of PostgreSQL's logical replication parallel apply worker system. It releases a stream lock that was previously acquired for a specific transaction. The function acts as a wrapper around UnlockApplyTransactionForSession, specifically targeting the PARALLEL_APPLY_LOCK_STREAM lock type. This mechanism ensures proper coordination between parallel apply workers when processing logical replication streams.

## Parameters / Member Variables
- `xid`: The transaction ID for which to unlock the stream lock
- `lockmode`: The lock mode that was used when acquiring the lock (must match the original lock mode)
## Dependencies
- Functions called/Symbols referenced:
  - [UnlockApplyTransactionForSession](../U/UnlockApplyTransactionForSession.md)
  - PARALLEL_APPLY_LOCK_STREAM
- Called from (representative examples):
  - [pa_process_spooled_messages_if_required](pa_process_spooled_messages_if_required.md)
  - [pa_decr_and_wait_stream_block](pa_decr_and_wait_stream_block.md)
  - [pa_xact_finish](pa_xact_finish.md)
  - [apply_handle_stream_start](../a/apply_handle_stream_start.md)
  - [apply_handle_stream_abort](../a/apply_handle_stream_abort.md)

## Notes and Other Information
- This function is specifically used in the context of parallel logical replication apply workers
- The lock being released is of type PARALLEL_APPLY_LOCK_STREAM, which coordinates stream access among parallel workers
- The function uses MyLogicalRepWorker->subid to identify the current worker's subscription
- Proper lock/unlock pairing is critical for avoiding deadlocks in the parallel apply system