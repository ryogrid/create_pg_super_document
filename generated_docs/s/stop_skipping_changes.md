# stop_skipping_changes

## Location
[src/backend/replication/logical/worker.c:4858-4879](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4858-L4879)

## Overview
stop_skipping_changes is a static function that terminates the transaction skipping mode in logical replication by resetting the skip state and logging the completion.

## Definition


## Detailed Description
This function serves as the counterpart to maybe_start_skipping_changes, providing a clean way to exit transaction skipping mode in logical replication. It checks if the system is currently in skipping mode and, if so, resets the skip_xact_finish_lsn to InvalidXLogRecPtr to disable skipping. The function also provides logging to track when transaction skipping has completed, which is important for debugging and monitoring logical replication behavior.

The function includes a safety check to avoid unnecessary operations if skipping is not currently active, making it safe to call regardless of the current skipping state.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - is_skipping_changes (state checking function)
  - ereport/errmsg (logging functions)
  - LSN_FORMAT_ARGS (LSN formatting macro)
  - InvalidXLogRecPtr (invalid LSN constant)

- Called from:
  - [apply_handle_prepare](../a/apply_handle_prepare.md) (in worker.c:1154)
  - [apply_handle_stream_prepare](../a/apply_handle_stream_prepare.md) (in worker.c:1396)
  - [apply_handle_commit_internal](../a/apply_handle_commit_internal.md) (in worker.c:2247)

## Notes and Other Information
- This is a static function, only accessible within worker.c
- Uses the global variable skip_xact_finish_lsn to control skipping state
- Provides detailed logging when skipping mode is disabled
- Safe to call multiple times or when not in skipping mode
- Typically called at transaction completion points (prepare, commit)
- Forms a pair with maybe_start_skipping_changes for complete skip control
- Essential for ensuring skipping mode doesn't persist beyond the intended transaction boundaries