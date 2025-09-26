# output_plugin_error_callback

## Location
[src/backend/replication/logical/logical.c:774-792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L774-L792)

## Overview
output_plugin_error_callback is a static error callback function that provides detailed context information when errors occur within logical replication output plugin callbacks.

## Definition
```c
static void output_plugin_error_callback(void *arg)
```

## Detailed Description
This function serves as an error context provider for logical replication output plugin operations. When errors occur during plugin callback execution, this function is invoked to add contextual information about the specific operation that failed. It provides detailed error context including the replication slot name, plugin name, callback name, and optionally the associated LSN (Log Sequence Number) where the error occurred.

The function formats error context messages that help administrators and developers identify exactly which plugin callback failed and at what point in the replication stream, making debugging logical replication issues significantly easier.

## Parameters / Member Variables
- `arg`: Void pointer to LogicalErrorCallbackState structure containing error context information

## Dependencies
- Functions called/Symbols referenced:
  - LogicalErrorCallbackState
  - errcontext
- Called from (representative examples):
  - startup_cb_wrapper
  - shutdown_cb_wrapper  
  - begin_cb_wrapper
  - commit_cb_wrapper
  - begin_prepare_cb_wrapper
  - prepare_cb_wrapper
  - commit_prepared_cb_wrapper
  - rollback_prepared_cb_wrapper
  - change_cb_wrapper
  - truncate_cb_wrapper
  - filter_prepare_cb_wrapper
  - filter_by_origin_cb_wrapper
  - message_cb_wrapper
  - stream_start_cb_wrapper
  - stream_stop_cb_wrapper
  - stream_abort_cb_wrapper
  - stream_prepare_cb_wrapper
  - stream_commit_cb_wrapper
  - stream_change_cb_wrapper
  - stream_message_cb_wrapper
  - stream_truncate_cb_wrapper
  - update_progress_txn_cb_wrapper

## Notes and Other Information
- The function conditionally includes LSN information in error messages when available (when report_location != InvalidXLogRecPtr)
- Error context format includes slot name, plugin name, callback name, and optionally LSN
- This callback is used extensively throughout all logical replication plugin wrapper functions
- Static function used internally within the logical replication subsystem
- Critical for debugging logical replication plugin issues in production environments