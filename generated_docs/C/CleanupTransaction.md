# CleanupTransaction

## Location
src/backend/access/transam/xact.c: 2945 - 2994

## Overview
CleanupTransaction performs the final cleanup phase after a transaction abort, resetting all transaction state back to default and releasing remaining resources.

## Definition


## Detailed Description
CleanupTransaction is the final step in the transaction abort process, called after AbortTransaction() has performed the main abort processing. It handles the cleanup operations that can only be safely performed after the transaction has been fully aborted and all critical resources have been released.

The function performs the final cleanup tasks:
- Validates that the transaction is in the expected TRANS_ABORT state
- Releases portal memory and transaction snapshots that were kept during abort processing
- Deletes the transaction resource owner and resets resource ownership pointers
- Calls AtCleanup_Memory() to perform comprehensive memory context cleanup
- Resets all transaction-related fields in the transaction state structure
- Transitions the transaction state from TRANS_ABORT back to TRANS_DEFAULT

This function complements the AtCleanup_* routines by ensuring that the transaction state structure is completely reset for the next transaction.

## Parameters / Member Variables
This function takes no parameters but operates on:
- : The current transaction's state structure
- : Must be TRANS_ABORT when called, transitions to TRANS_DEFAULT
- : Resource owner that gets deleted and cleared
- Transaction state fields: All transaction-specific fields are reset to default values

## Dependencies
- Functions called/Symbols referenced:
  - TransStateAsString (for error reporting if state is invalid)
  - AtCleanup_Portals (safe portal memory cleanup after abort)
  - AtEOXact_Snapshot (release transaction snapshots with cleanup flag)
  - ResourceOwnerDelete (delete the top-level transaction resource owner)
  - AtCleanup_Memory (comprehensive transaction memory cleanup)

- Called from (representative examples):
  - CommitTransactionCommandInternal (cleanup after failed commit or successful abort)
  - AbortCurrentTransactionInternal (various abort scenarios)
  - AbortOutOfAnyTransaction (emergency cleanup from any transaction state)

## Notes and Other Information
- Must only be called when transaction state is TRANS_ABORT - will FATAL error otherwise
- Serves as the counterpart to StartTransaction() by resetting all state it initializes
- The portal and snapshot cleanup here is safer than during AbortTransaction() because all other resources have been released
- Resets both the main transaction fields and parallel transaction tracking variables
- After this function completes, the backend is ready to start a new transaction
- Part of the three-phase abort sequence: AbortTransaction() → CleanupTransaction() → ready for new transaction