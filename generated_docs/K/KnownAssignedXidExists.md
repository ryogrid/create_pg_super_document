# KnownAssignedXidExists

## Location
src/backend/storage/ipc/procarray.c: 4973 - 4985

## Overview
Checks whether a specific transaction ID is present in the KnownAssignedXids array without modifying the array.

## Definition
static bool KnownAssignedXidExists(TransactionId xid)

## Detailed Description
KnownAssignedXidExists is a simple wrapper function that provides read-only access to check if a transaction ID exists in the KnownAssignedXids array. It delegates the actual search operation to KnownAssignedXidsSearch with the remove parameter set to false, ensuring the array remains unchanged. The function includes an assertion to validate that the provided transaction ID is valid before performing the search.

This function is typically used during transaction visibility checks to determine if a transaction ID corresponds to a known assigned transaction on a standby server.

## Parameters / Member Variables
- xid: The transaction ID to check for existence in the KnownAssignedXids array

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsValid (via Assert)
  - KnownAssignedXidsSearch
- Called from (representative examples):
  - TransactionIdIsInProgress

## Notes and Other Information
- Caller must hold ProcArrayLock in shared or exclusive mode
- This is a read-only operation that does not modify the KnownAssignedXids array
- Includes assertion to ensure the transaction ID is valid before searching
- Used primarily in transaction visibility determination on standby servers