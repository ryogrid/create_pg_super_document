# maybe_start_skipping_changes

## Location
[src/backend/replication/logical/worker.c:4831-4857](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4831-L4857)

## Overview
maybe_start_skipping_changes is a static function that conditionally initiates transaction skipping in logical replication when the transaction's finish LSN matches the subscription's configured skip LSN.

## Definition

```c
static void
maybe_start_skipping_changes(XLogRecPtr finish_lsn)
```
## Detailed Description
This function implements a selective transaction skipping mechanism for logical replication. It evaluates whether a transaction should be skipped based on comparing the transaction's finish LSN with the subscription's skip LSN configuration. When a match is found, the function activates the skipping mode for the entire transaction, which allows logical replication to bypass problematic transactions that might cause replication failures.

The function includes several safety assertions to ensure it's called in the correct context:
- Not already in skipping mode
- Not currently in a remote transaction
- Not currently in a streamed transaction

The function uses a fast-path optimization with the likely() macro, assuming that transaction skipping is not commonly used, allowing the normal case to execute efficiently.

## Parameters / Member Variables
- `finish_lsn`: The LSN (Log Sequence Number) representing the end position of the transaction being evaluated
## Dependencies
- Functions called/Symbols referenced:
  - is_skipping_changes (state checking function)
  - XLogRecPtrIsInvalid (LSN validation macro)
  - likely (compiler optimization hint macro)
  - Assert (debugging assertion macro)
  - ereport/errmsg (logging functions)
  - LSN_FORMAT_ARGS (LSN formatting macro)

- Called from:
  - [apply_handle_begin](../a/apply_handle_begin.md) (in worker.c:1005)
  - [apply_handle_begin_prepare](../a/apply_handle_begin_prepare.md) (in worker.c:1062)
  - [apply_spooled_messages](../a/apply_spooled_messages.md) (in worker.c:2015)

## Notes and Other Information
- This is a static function, only accessible within worker.c
- Uses global variables MySubscription and skip_xact_finish_lsn
- The function provides detailed logging when transaction skipping is activated
- Transaction skipping is typically used as a recovery mechanism for problematic transactions
- The fast-path optimization assumes skipping is rarely needed in normal operations
- The function must be called at transaction boundaries for proper functionality