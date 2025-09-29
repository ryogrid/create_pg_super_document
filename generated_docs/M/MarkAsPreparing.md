# MarkAsPreparing

## Location
[src/backend/access/transam/twophase.c:359-432](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L359-L432)

## Overview
Reserves a Global Identifier (GID) for a two-phase commit transaction, transitioning it to the preparing state within the PostgreSQL two-phase commit system.

## Definition

```c
struct and puts it into the active array.
 * NOTE: this is also used when reloading a gxact after a crash;
```
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
  - [GlobalTransaction](../G/GlobalTransaction.md)
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

## Simplified Source

```c
GlobalTransaction MarkAsPreparing(TransactionId xid, const char *gid,
                                  TimestampTz prepared_at, Oid owner, Oid databaseid)
{
    GlobalTransaction gxact;
    int i;

    // Validate GID length
    if (strlen(gid) >= GIDSIZE)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("transaction identifier \"%s\" is too long", gid)));

    // Check if prepared transactions are enabled
    if (max_prepared_xacts == 0)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("prepared transactions are disabled"),
                 errhint("Set \"max_prepared_transactions\" to a nonzero value.")));

    // Register exit hook on first call
    if (!twophaseExitRegistered)
    {
        before_shmem_exit(AtProcExit_Twophase, 0);
        twophaseExitRegistered = true;
    }

    LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);

    // Check for conflicting GID in existing prepared transactions
    for (i = 0; i < TwoPhaseState->numPrepXacts; i++)
    {
        gxact = TwoPhaseState->prepXacts[i];
        if (strcmp(gxact->gid, gid) == 0)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_DUPLICATE_OBJECT),
                     errmsg("transaction identifier \"%s\" is already in use", gid)));
        }
    }

    // Get a free GlobalTransaction from the freelist
    if (TwoPhaseState->freeGXacts == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("maximum number of prepared transactions reached"),
                 errhint("Increase \"max_prepared_transactions\" (currently %d).",
                         max_prepared_xacts)));

    gxact = TwoPhaseState->freeGXacts;
    TwoPhaseState->freeGXacts = gxact->next;

    // Initialize the GlobalTransaction structure
    MarkAsPreparingGuts(gxact, xid, gid, prepared_at, owner, databaseid);

    gxact->ondisk = false;

    // Add to the active prepared transactions array
    Assert(TwoPhaseState->numPrepXacts < max_prepared_xacts);
    TwoPhaseState->prepXacts[TwoPhaseState->numPrepXacts++] = gxact;

    LWLockRelease(TwoPhaseStateLock);

    return gxact;
}
```