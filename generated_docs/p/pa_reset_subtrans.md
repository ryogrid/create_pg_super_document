# pa_reset_subtrans

## Location
[src/backend/replication/logical/applyparallelworker.c:1402-1415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L1402-L1415)

## Overview
pa_reset_subtrans is a utility function in PostgreSQL's logical replication parallel worker module that resets the list of subtransactions, clearing all tracked subtransaction state.

## Definition

```c
void
pa_reset_subtrans(void)
```
## Detailed Description
This function provides a simple mechanism to reset the subtransaction tracking list (subxactlist) by setting it to NIL. It is specifically designed for use in parallel apply workers during logical replication operations. The function relies on PostgreSQL's transaction-scoped memory management, where the memory allocated for the subtransaction list will be automatically freed when the current transaction ends, eliminating the need for explicit memory deallocation.

The function is typically called during error recovery scenarios or when cleaning up after stream processing operations to ensure that no stale subtransaction state persists between operations.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - NIL (PostgreSQL list constant)
  - subxactlist (global variable)
- Called from (representative examples):
  - [pa_stream_abort](pa_stream_abort.md)
  - [apply_handle_stream_prepare](../a/apply_handle_stream_prepare.md)
  - [apply_handle_stream_commit](../a/apply_handle_stream_commit.md)

## Notes and Other Information
- The function does not perform explicit memory deallocation, relying instead on PostgreSQL's transaction-scoped memory management
- Used primarily in logical replication parallel worker contexts
- Part of the cleanup process during stream abort, prepare, and commit operations
- The subxactlist global variable maintains the list of subtransactions being tracked by the parallel worker