# pa_find_worker

## Location
src/backend/replication/logical/applyparallelworker.c: 518 - 555

## Overview
Locates and returns the parallel apply worker assigned to a specific transaction ID.

## Definition


## Detailed Description
This function provides a lookup mechanism to find the parallel apply worker assigned to a given transaction. It implements a caching strategy by first checking if there's a cached worker (stream_apply_worker), then searching the hash table for the transaction-to-worker mapping. The function includes validation to ensure the transaction ID is valid and that the hash table exists before performing the lookup.

## Parameters / Member Variables
- : The transaction ID to search for in the worker assignments

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsValid
  - hash_search
  - Assert
- Called from:
  - get_transaction_apply_action

## Notes and Other Information
- Returns NULL for invalid transaction IDs or if no hash table exists
- Uses stream_apply_worker as a cache to avoid hash table lookup when possible
- Performs hash table lookup using HASH_FIND operation when no cached worker available
- Includes assertion to verify found worker is still in use (hasn't exited)
- Returns ParallelApplyWorkerInfo pointer on success, NULL if no worker found
- Part of the worker lookup infrastructure for PostgreSQL's logical replication parallel processing
- Located in src/backend/replication/logical/applyparallelworker.c:518-555