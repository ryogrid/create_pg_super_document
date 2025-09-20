# reset_apply_error_context_info

## Location
[src/backend/replication/logical/worker.c:5049-5064](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L5049-L5064)

## Overview
Resets all fields in the global error callback context structure to their initial/invalid states, clearing any previously set error context information for logical replication.

## Definition
static inline void reset_apply_error_context_info(void)

## Detailed Description
This function serves as a cleanup utility that resets the global apply_error_callback_arg structure to a clean state. It's called after completing processing of logical replication operations to ensure that stale error context information doesn't carry over to subsequent operations.

The function performs a complete reset of all error context fields:
1. Sets the command field to 0 (invalid/no command)
2. Clears the relation pointer (sets to NULL)
3. Resets the remote_attnum to -1 (invalid attribute number)
4. Calls set_apply_error_context_xact() to reset transaction information with invalid values

This comprehensive reset ensures that if an error occurs in subsequent operations, the error callback won't report misleading context information from previous operations. It's an important part of maintaining clean error reporting state in the logical replication worker.

The function is marked as static inline for performance optimization, as it's frequently called during the logical replication process.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [set_apply_error_context_xact](../s/set_apply_error_context_xact.md) (to reset transaction context with invalid values)
- Called from (representative examples):
  - [apply_handle_commit](../a/apply_handle_commit.md) (after transaction commit)
  - [apply_handle_prepare](../a/apply_handle_prepare.md) (after transaction preparation)
  - [apply_handle_commit_prepared](../a/apply_handle_commit_prepared.md) (after prepared transaction commit)
  - [apply_handle_rollback_prepared](../a/apply_handle_rollback_prepared.md) (after prepared transaction rollback)
  - [apply_handle_stream_prepare](../a/apply_handle_stream_prepare.md) (after streaming preparation)
  - [apply_handle_stream_stop](../a/apply_handle_stream_stop.md) (when stopping streaming)
  - [apply_handle_stream_abort](../a/apply_handle_stream_abort.md) (after streaming abort)
  - [apply_handle_stream_commit](../a/apply_handle_stream_commit.md) (after streaming commit)

## Notes and Other Information
- This is a static inline function for optimal performance
- Provides complete cleanup of the global apply_error_callback_arg structure
- Essential for preventing error context information from bleeding between operations
- Called at the completion of various transaction lifecycle operations
- Uses InvalidTransactionId and InvalidXLogRecPtr constants for resetting transaction context
- Part of the error reporting infrastructure cleanup for logical replication
- Located in src/backend/replication/logical/worker.c:5049-5064