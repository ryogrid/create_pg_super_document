# _SPI_rollback

## Location
[src/backend/executor/spi.c:332-412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L332-L412)

## Overview
_SPI_rollback is the internal implementation function that handles transaction rollback within the SPI context, supporting both regular rollback and chained rollback operations.

## Definition

```c
static void
_SPI_rollback(bool chain)
```
## Detailed Description
_SPI_rollback is the core internal function that implements transaction rollback functionality for the SPI (Server Programming Interface). This static function serves as the implementation backend for both SPI_rollback and SPI_rollback_and_chain, with the behavior controlled by the 'chain' parameter.

The function performs comprehensive validation and cleanup:
1. Validates that the current SPI context permits transaction termination (not in atomic mode)
2. Ensures no subtransaction is active, preventing violation of subtransaction semantics
3. Optionally saves transaction characteristics if chaining is requested
4. Protects portals by holding pinned portals and releasing snapshots before state changes
5. Aborts the current transaction and immediately starts a new one
6. Restores transaction characteristics if chaining was requested

The entire operation is wrapped in a PG_TRY/PG_CATCH block to handle errors during rollback. If the rollback itself fails, the function attempts to abort again and ensures a new transaction is started, maintaining database consistency.

## Parameters / Member Variables
- `chain`: boolean parameter that determines whether transaction characteristics should be preserved across the rollback boundary
## Dependencies
- Functions called/Symbols referenced:
  - [SavedTransactionCharacteristics](SavedTransactionCharacteristics.md) (transaction state structure)
  - [IsSubTransaction](../I/IsSubTransaction.md) (check for active subtransaction)
  - [SaveTransactionCharacteristics](SaveTransactionCharacteristics.md) (save current transaction properties)
  - [HoldPinnedPortals](../H/HoldPinnedPortals.md) (protect portals during transaction boundary)
  - [ForgetPortalSnapshots](../F/ForgetPortalSnapshots.md) (release portal snapshots)
  - [AbortCurrentTransaction](../A/AbortCurrentTransaction.md) (abort the current transaction)
  - [StartTransactionCommand](StartTransactionCommand.md) (start new transaction)
  - [RestoreTransactionCharacteristics](../R/RestoreTransactionCharacteristics.md) (restore transaction properties when chaining)
  - [CopyErrorData](../C/CopyErrorData.md)/FlushErrorState/ReThrowError (error handling)
- Called from:
  - [SPI_rollback](SPI_rollback.md) (with chain=false)
  - [SPI_rollback_and_chain](SPI_rollback_and_chain.md) (with chain=true)

## Notes and Other Information
- This is a static (internal) function not exposed in the public SPI API
- Shares similar structure and error handling patterns with _SPI_commit
- Cannot be called in atomic SPI contexts or when subtransactions are active
- The chain parameter implements SQL standard ROLLBACK AND CHAIN semantics
- Error handling ensures that even if rollback fails, a new transaction is established
- Memory context switching ensures proper cleanup during error conditions
- The internal_xact flag protects the SPI stack entry during transaction state changes

## Simplified Source

```c
static void _SPI_rollback(bool chain) {
    MemoryContext oldcontext = CurrentMemoryContext;
    SavedTransactionCharacteristics savetc;

    // Validate SPI context allows transaction termination
    if (_SPI_current->atomic)
        ereport(ERROR, "invalid transaction termination");

    // Ensure no subtransaction is active
    if (IsSubTransaction())
        ereport(ERROR, "cannot roll back while a subtransaction is active");

    // Save transaction characteristics if chaining
    if (chain)
        SaveTransactionCharacteristics(&savetc);

    PG_TRY(); {
        // Protect SPI stack entry
        _SPI_current->internal_xact = true;

        // Hold portals and release snapshots before transaction change
        HoldPinnedPortals();
        ForgetPortalSnapshots();

        // Abort current transaction and start new one
        AbortCurrentTransaction();
        StartTransactionCommand();

        // Restore characteristics if chaining
        if (chain)
            RestoreTransactionCharacteristics(&savetc);

        MemoryContextSwitchTo(oldcontext);
        _SPI_current->internal_xact = false;
    }
    PG_CATCH(); {
        // Handle rollback errors: save error, retry abort, restart transaction
        ErrorData *edata;
        MemoryContextSwitchTo(oldcontext);
        edata = CopyErrorData();
        FlushErrorState();

        AbortCurrentTransaction();
        StartTransactionCommand();
        if (chain)
            RestoreTransactionCharacteristics(&savetc);

        MemoryContextSwitchTo(oldcontext);
        _SPI_current->internal_xact = false;
        ReThrowError(edata);
    }
    PG_END_TRY();
}
```