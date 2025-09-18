# GetPreparedTransactionList

## Location
src/backend/access/transam/twophase.c: 666 - 700

## Overview
GetPreparedTransactionList creates a copy of all prepared transactions from shared memory for use by the pg_prepared_xact system view.

## Definition


## Detailed Description
GetPreparedTransactionList is a static function that provides a snapshot of all currently prepared transactions in the system. It returns an array containing copies of all GlobalTransaction structures from the shared memory TwoPhaseState, minimizing the time spent holding TwoPhaseStateLock. The function allocates memory for the returned array using palloc() and performs deep copies of each transaction structure to ensure data consistency. The function may return transactions that are not yet fully prepared, so callers should filter appropriately if needed.

## Parameters / Member Variables
- : Output parameter - pointer to receive the allocated array of GlobalTransaction structures

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire/LWLockRelease (for shared lock on TwoPhaseStateLock)  
  - palloc (for memory allocation)
  - memcpy (for copying transaction data)
- Data structures accessed:
  - TwoPhaseState (global two-phase commit state)
  - GlobalTransaction/GlobalTransactionData (transaction structure types)
- Called from:
  - pg_prepared_xact (system view function)

## Notes and Other Information
- Returns the number of prepared transactions as the function result
- Sets *gxacts to NULL and returns 0 if no prepared transactions exist
- Uses shared locking on TwoPhaseStateLock to minimize contention
- Creates complete copies of transaction data to avoid holding locks during caller processing
- The returned array is palloc'd and should be freed by the caller when no longer needed
- WARNING: May include transactions that are not fully prepared yet - caller filtering may be required