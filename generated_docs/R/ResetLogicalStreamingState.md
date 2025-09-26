# ResetLogicalStreamingState

## Location
src/backend/replication/logical/logical.c: 1969 - 1978

## Overview
Clears logical streaming state variables during transaction or subtransaction abort to reset the system to a clean state.

## Definition
void ResetLogicalStreamingState(void)

## Detailed Description
This function is responsible for resetting global state variables used during logical replication streaming when a transaction or subtransaction aborts. It ensures that any partially processed logical replication state is properly cleared to prevent inconsistencies or stale data from affecting subsequent operations.

The function resets two critical global variables:
1. CheckXidAlive - tracks transaction IDs that need to be validated as still active
2. bsysscan - indicates whether a bootstrap system scan is in progress

This cleanup is essential during abort scenarios to ensure that the logical replication subsystem returns to a consistent state and doesn't carry over partially processed transaction information.

## Parameters / Member Variables
None - this is a void function with no parameters

## Dependencies
- Functions called/Symbols referenced:
  - InvalidTransactionId (constant)
  - Global variables: CheckXidAlive, bsysscan
- Called from (representative examples):
  - AbortTransaction
  - AbortSubTransaction

## Notes and Other Information
- Simple cleanup function with minimal overhead
- Critical for maintaining logical replication consistency during error conditions
- Part of the transaction abort cleanup protocol
- The CheckXidAlive variable is declared in src/backend/access/transam/xact.c
- The bsysscan variable is also declared in src/backend/access/transam/xact.c and used by the index scanning subsystem