# MarkAsPreparing

## Location
src/backend/access/transam/twophase.c: 359 - 432

## Overview
Reserves a Global Identifier (GID) for a two-phase commit transaction, transitioning it to the preparing state within the PostgreSQL two-phase commit system.

## Definition


## Detailed Description
MarkAsPreparing is a core function in PostgreSQL's two-phase commit protocol that manages the transition of a transaction to the preparing state. It performs several critical validations and setup tasks: validates the GID length and uniqueness, ensures the two-phase commit feature is enabled, registers the exit hook for cleanup, and allocates a GlobalTransaction structure from the free list. The function operates under exclusive lock protection to ensure thread safety and maintains the prepared transaction state in shared memory.

## Parameters / Member Variables
- : The transaction ID being prepared for two-phase commit
- : The Global Identifier string for the transaction (must be unique and under GIDSIZE length)
- : Timestamp when the transaction was prepared
- : Object ID of the user who owns this prepared transaction
- : Object ID of the database where this transaction is being prepared

## Dependencies
- Functions called/Symbols referenced:
  - GlobalTransaction
  - GIDSIZE
  - [AtProcExit_Twophase](../A/AtProcExit_Twophase.md)
  - [before_shmem_exit](../b/before_shmem_exit.md)
  - ERRCODE_DUPLICATE_OBJECT
  - [MarkAsPreparingGuts](MarkAsPreparingGuts.md)
- Called from (representative examples):
  - [PrepareTransaction](../P/PrepareTransaction.md)

## Notes and Other Information
- Requires max_prepared_xacts to be set to a non-zero value to function
- Performs GID conflict detection by scanning all existing prepared transactions
- Automatically registers the AtProcExit_Twophase exit hook on first call
- Uses TwoPhaseStateLock for thread-safe access to shared state
- Returns the allocated GlobalTransaction structure for further processing
- Sets the ondisk flag to false initially (transaction not yet persisted)