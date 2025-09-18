# KnownAssignedXidsGet

## Location
src/backend/storage/ipc/procarray.c: 5112 - 5125

## Overview
KnownAssignedXidsGet retrieves an array of known assigned transaction IDs by scanning the KnownAssignedXids structure, filtering out any transaction IDs that are greater than or equal to a specified maximum value.

## Definition


## Detailed Description
This function serves as a simplified wrapper around KnownAssignedXidsGetAndSetXmin, providing a way to retrieve known assigned transaction IDs without setting the minimum transaction ID. It scans the KnownAssignedXids data structure and populates the provided array with transaction IDs that are less than the specified maximum value (xmax). The function is typically used during transaction visibility checks and snapshot creation in PostgreSQL's MVCC implementation.

The function requires the caller to hold the ProcArrayLock in at least shared mode to ensure consistent access to the shared KnownAssignedXids structure.

## Parameters / Member Variables
- : Output array where the retrieved transaction IDs will be stored. The caller is responsible for ensuring this array is large enough to hold all qualifying transaction IDs.
- : Maximum transaction ID threshold. Any transaction IDs greater than or equal to this value will be filtered out from the results.

## Dependencies
- Functions called/Symbols referenced:
  - KnownAssignedXidsGetAndSetXmin
  - InvalidTransactionId (constant)
- Called from (representative examples):
  - xc_slow_answer_inc
  - TransactionIdIsInProgress

## Notes and Other Information
- This is a static function, meaning it's only accessible within the procarray.c file
- The function internally creates a temporary TransactionId variable (xtmp) initialized to InvalidTransactionId and passes it to KnownAssignedXidsGetAndSetXmin
- Caller must ensure proper locking (ProcArrayLock in shared mode minimum) before calling this function
- The function returns the number of transaction IDs stored in the output array
- This function is part of PostgreSQL's Hot Standby implementation for managing known assigned transaction IDs on standby servers