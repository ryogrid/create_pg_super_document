# KnownAssignedXidsRemove

## Location
[src/backend/storage/ipc/procarray.c:4986-5011](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L4986-L5011)

## Overview
Removes a specific transaction ID from the KnownAssignedXids array by delegating to the search function with removal enabled.

## Definition
static void KnownAssignedXidsRemove(TransactionId xid)

## Detailed Description
KnownAssignedXidsRemove provides a clean interface for removing transaction IDs from the KnownAssignedXids array. The function is designed to be tolerant of attempts to remove non-existent XIDs, which can occur during normal operation when processing subtransaction assignments. Specifically, subtransaction IDs may be removed preemptively during XLOG_XACT_ASSIGNMENT processing to prevent array overflow, and then removed again when the top-level transaction commits or aborts.

The function includes debug logging at level 4 to track removal operations and deliberately ignores the return value from KnownAssignedXidsSearch to avoid treating missing XIDs as errors.

## Parameters / Member Variables
- xid: The transaction ID to remove from the KnownAssignedXids array

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsValid (via Assert)
  - elog (with DEBUG4 level)
  - KnownAssignedXidsSearch
- Called from (representative examples):
  - KnownAssignedXidsRemoveTree

## Notes and Other Information
- Caller must hold ProcArrayLock in exclusive mode
- Tolerates removal of non-existent XIDs without error, which is intentional behavior
- Includes DEBUG4 logging for troubleshooting removal operations
- Commonly used during subtransaction cleanup and top-level transaction completion
- The function explicitly ignores the search result to handle duplicate removal attempts gracefully