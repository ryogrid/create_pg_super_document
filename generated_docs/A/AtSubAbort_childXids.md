# AtSubAbort_childXids

## Location
src/backend/access/transam/xact.c: 1911 - 1942

## Overview
AtSubAbort_childXids is a static function that cleans up child transaction ID arrays when a subtransaction is aborted, freeing allocated memory and resetting related counters.

## Definition
static void AtSubAbort_childXids(void)

## Detailed Description
This function is responsible for cleaning up child transaction ID (XID) tracking data structures when a subtransaction is being aborted. It performs memory management for the childXids array that tracks all child transaction IDs associated with the current transaction.

The function frees the dynamically allocated childXids array and resets all related counters (nChildXids and maxChildXids) to zero. This prevents memory leaks during subtransaction abort processing. The child-XID arrays are kept in TopTransactionContext, so explicit cleanup is necessary during abort to avoid memory leakage.

Notably, the function does not prune the unreportedXids array, as mentioned in the code comments. This design choice prioritizes performance in common execution paths over potential reduction in XLOG_XACT_ASSIGNMENT records.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - CurrentTransactionState (global variable)
  - TransactionState (type)
  - pfree (memory deallocation function)
- Called from (representative examples):
  - AbortSubTransaction

## Notes and Other Information
- This function is static and only used within the transaction management subsystem
- Part of the coordinated subtransaction abort cleanup process
- The childXids array is stored in TopTransactionContext for memory management efficiency
- The function deliberately does not prune unreportedXids array for performance reasons
- Memory cleanup is essential to prevent leaks since abort paths may not follow normal cleanup procedures
- Works in conjunction with other AtSubAbort_* functions during subtransaction cleanup