# clear_subscription_skip_lsn

## Location
src/backend/replication/logical/worker.c: 4880 - 4968

## Overview
Clears the subskiplsn field in the pg_subscription catalog for the current subscription, with validation to ensure the skip LSN matches the expected finish LSN of a transaction.

## Definition
static void clear_subscription_skip_lsn(XLogRecPtr finish_lsn)

## Detailed Description
This function is responsible for clearing the subskiplsn (subscription skip LSN) field in the pg_subscription catalog table. The subskiplsn is used in logical replication to skip problematic transactions during replication. When a transaction completes successfully or needs to be cleared, this function removes the skip LSN setting.

The function performs several important validations and safety checks:
- It only operates if there is a valid skip LSN set and the worker is not a parallel apply worker
- It manages transaction state, starting a new transaction if needed
- It uses proper locking to prevent concurrent modifications of the subscription
- It validates that the current skip LSN matches the provided finish_lsn before clearing it
- If the LSNs don't match, it issues a warning to inform users of potential mismatches

The function follows PostgreSQL's standard patterns for catalog updates, including proper snapshot management, locking, and tuple modification procedures.

## Parameters / Member Variables
- : The LSN of the transaction that has finished processing, used to validate against the current subskiplsn before clearing it

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtrIsInvalid (checks if LSN is invalid)
  - am_parallel_apply_worker (checks if this is a parallel worker)
  - IsTransactionState (checks transaction state)
  - StartTransactionCommand (starts new transaction if needed)
  - GetTransactionSnapshot/PushActiveSnapshot (snapshot management)
  - LockSharedObject (prevents concurrent subscription updates)
  - SearchSysCacheCopy1 (retrieves subscription tuple)
  - heap_modify_tuple (modifies the catalog tuple)
  - CatalogTupleUpdate (updates the catalog)
  - heap_freetuple (frees tuple memory)
  - PopActiveSnapshot (pops snapshot)
  - CommitTransactionCommand (commits transaction if started)
- Called from (representative examples):
  - apply_handle_prepare
  - apply_handle_commit_prepared
  - apply_handle_rollback_prepared
  - apply_handle_stream_prepare
  - apply_handle_commit_internal

## Notes and Other Information
- This is a static function only used within worker.c
- The function handles cases where the skip LSN might have been changed by another process, issuing warnings but not failing
- Proper transaction and snapshot management ensures data consistency
- The function skips operation for parallel apply workers as they don't manage subscription state directly
- Located in src/backend/replication/logical/worker.c:4880-4968