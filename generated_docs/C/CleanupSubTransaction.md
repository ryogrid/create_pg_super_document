# CleanupSubTransaction

## Location
[src/backend/access/transam/xact.c:5321-5353](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L5321-L5353)

## Overview
CleanupSubTransaction performs final cleanup of a subtransaction that has been aborted, removing its transaction state and transferring control back to the parent transaction.

## Definition


## Detailed Description
CleanupSubTransaction is a static function that performs the final cleanup phase of a subtransaction that has been aborted. This function is typically called after AbortSubTransaction to complete the cleanup process:

1. **State Validation**: Verifies that the subtransaction is in TRANS_ABORT state before proceeding
2. **Portal Cleanup**: Calls AtSubCleanup_Portals to clean up any portals associated with the subtransaction
3. **Resource Owner Management**: 
   - Restores the current resource owner to the parent transaction's resource owner
   - Deletes the subtransaction's resource owner if it exists
   - Sets the subtransaction's resource owner pointer to NULL
4. **Memory Cleanup**: Calls AtSubCleanup_Memory to perform memory context cleanup
5. **State Finalization**: Changes the transaction state to TRANS_DEFAULT
6. **Transaction Removal**: Calls PopTransaction to remove the subtransaction from the transaction stack

This function completes the three-phase subtransaction abort process (abort → cleanup → removal) and ensures that all subtransaction-specific resources are properly cleaned up before returning control to the parent transaction.

## Parameters / Member Variables
This function takes no parameters and operates on the global CurrentTransactionState.

## Dependencies
- Functions called/Symbols referenced:
  - [ShowTransactionState](../S/ShowTransactionState.md)
  - [TransStateAsString](../T/TransStateAsString.md)
  - [AtSubCleanup_Portals](../A/AtSubCleanup_Portals.md)
  - [ResourceOwnerDelete](../R/ResourceOwnerDelete.md)
  - [AtSubCleanup_Memory](../A/AtSubCleanup_Memory.md)
  - [PopTransaction](../P/PopTransaction.md)
- Called from (representative examples):
  - [CommitTransactionCommandInternal](CommitTransactionCommandInternal.md)
  - [AbortCurrentTransactionInternal](../A/AbortCurrentTransactionInternal.md)
  - [RollbackAndReleaseCurrentSubTransaction](../R/RollbackAndReleaseCurrentSubTransaction.md)
  - [AbortOutOfAnyTransaction](../A/AbortOutOfAnyTransaction.md)

## Notes and Other Information
- The function includes a warning that callers must reassign CurrentTransactionState local pointers after calling this function, similar to CommitSubTransaction
- This function should only be called on subtransactions that are in TRANS_ABORT state; calling it in other states will generate a warning
- The function is designed to be lightweight and focused only on final cleanup, as the heavy lifting of abort processing is done by AbortSubTransaction
- Unlike CommitSubTransaction and AbortSubTransaction, this function performs minimal operations and focuses primarily on resource owner cleanup and state management
- The function is part of the standard subtransaction abort sequence: AbortSubTransaction() followed by CleanupSubTransaction()
- Located in src/backend/access/transam/xact.c:5321-5353