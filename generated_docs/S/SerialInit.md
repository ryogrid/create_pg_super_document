# SerialInit

## Location
src/backend/storage/lmgr/predicate.c: 806 - 846

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
  - `SerialPagePrecedesLogically`
  - `SimpleLruInit`
  - `SerialPagePrecedesLogicallyUnitTests`
  - `SlruPagePrecedesUnitTests`
  - `ShmemInitStruct`
  - `LWLockAcquire`/`LWLockRelease`
  - `LWTRANCHE_SERIAL_BUFFER`
  - `LWTRANCHE_SERIAL_SLRU`
  - `SYNC_HANDLER_NONE`
  - `SERIAL_ENTRIESPERPAGE`
- Called from (representative examples):
  - `InitPredicateLocks`

## Notes and Other Information
- Called during PostgreSQL startup to initialize the serializable isolation subsystem
- The function distinguishes between postmaster and backend processes using IsUnderPostmaster
- Only the postmaster process initializes the control structure with empty state
- Uses exclusive locking (SerialControlLock) when initializing control structure state
- The SLRU is configured with no sync handler (SYNC_HANDLER_NONE) and no directory creation (false flag)
- Unit tests are automatically run in debug builds to ensure correctness of the page precedence logic