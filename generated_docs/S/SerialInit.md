# SerialInit

## Location
[src/backend/storage/lmgr/predicate.c:806-846](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L806-L846)

## Overview
Initializes the tracking system for old serializable committed transaction IDs, setting up SLRU management and control structures.

## Definition
```c
static void SerialInit(void)
```

## Detailed Description
This function performs the complete initialization of PostgreSQL's serializable isolation tracking system. It sets up the Simple Least Recently Used (SLRU) buffer management for the pg_serial data and initializes the control structures needed to track committed serializable transactions.

The initialization process involves:
1. **SLRU Setup**: Configures the SerialSlruCtl with the SerialPagePrecedesLogically function as the page precedence callback
2. **Buffer Management**: Initializes the SLRU with serializable_buffers, lightweight tranche locks, and sync handling
3. **Unit Testing**: Runs comprehensive unit tests when USE_ASSERT_CHECKING is enabled
4. **Shared Memory**: Creates or attaches to the SerialControlData shared memory structure
5. **Control Structure**: Initializes the control structure with empty SLRU state if this is the first initialization

## Parameters / Member Variables
This function takes no parameters but initializes several key components:
- SerialSlruCtl: The SLRU control structure for pg_serial data
- serialControl: Shared memory control structure for serial tracking
- headPage: Initially set to -1 (empty)
- headXid: Initially set to InvalidTransactionId
- tailXid: Initially set to InvalidTransactionId

## Dependencies
- Functions called/Symbols referenced:
  - `[SerialPagePrecedesLogically](SerialPagePrecedesLogically.md)`
  - `[SimpleLruInit](SimpleLruInit.md)`
  - `[SerialPagePrecedesLogicallyUnitTests](SerialPagePrecedesLogicallyUnitTests.md)`
  - `[SlruPagePrecedesUnitTests](SlruPagePrecedesUnitTests.md)`
  - `[ShmemInitStruct](ShmemInitStruct.md)`
  - `[LWLockAcquire](../L/LWLockAcquire.md)`/`LWLockRelease`
  - `LWTRANCHE_SERIAL_BUFFER`
  - `LWTRANCHE_SERIAL_SLRU`
  - `SYNC_HANDLER_NONE`
  - `SERIAL_ENTRIESPERPAGE`
- Called from (representative examples):
  - `[InitPredicateLocks](../I/InitPredicateLocks.md)`

## Notes and Other Information
- Called during PostgreSQL startup to initialize the serializable isolation subsystem
- The function distinguishes between postmaster and backend processes using IsUnderPostmaster
- Only the postmaster process initializes the control structure with empty state
- Uses exclusive locking (SerialControlLock) when initializing control structure state
- The SLRU is configured with no sync handler (SYNC_HANDLER_NONE) and no directory creation (false flag)
- Unit tests are automatically run in debug builds to ensure correctness of the page precedence logic

## Simplified Source

```c
// Simplified version of SerialInit
static void SerialInit(void) {
    bool found;

    // Set up SLRU management for pg_serial data
    SerialSlruCtl->PagePrecedes = SerialPagePrecedesLogically;
    SimpleLruInit(SerialSlruCtl, "serializable",
                  serializable_buffers, 0, "pg_serial",
                  LWTRANCHE_SERIAL_BUFFER, LWTRANCHE_SERIAL_SLRU,
                  SYNC_HANDLER_NONE, false);

#ifdef USE_ASSERT_CHECKING
    // Run unit tests in debug builds
    SerialPagePrecedesLogicallyUnitTests();
#endif
    SlruPagePrecedesUnitTests(SerialSlruCtl, SERIAL_ENTRIESPERPAGE);

    // Create or attach to the SerialControl structure
    serialControl = (SerialControl)
        ShmemInitStruct("SerialControlData", sizeof(SerialControlData), &found);

    Assert(found == IsUnderPostmaster);
    if (!found) {
        // Initialize control structure to reflect empty SLRU (postmaster only)
        LWLockAcquire(SerialControlLock, LW_EXCLUSIVE);
        serialControl->headPage = -1;
        serialControl->headXid = InvalidTransactionId;
        serialControl->tailXid = InvalidTransactionId;
        LWLockRelease(SerialControlLock);
    }
}
```

Key simplifications made:
- Added clear comments for each major initialization phase
- Grouped related operations together logically
- Clarified the distinction between postmaster and backend initialization
- Maintained all essential functionality including debug unit tests