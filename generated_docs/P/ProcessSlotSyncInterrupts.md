# ProcessSlotSyncInterrupts

## Location
[src/backend/replication/logical/slotsync.c:1155-1176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/slotsync.c#L1155-L1176)

## Overview
ProcessSlotSyncInterrupts is an interrupt handler function for the main loop of the slot synchronization worker that processes shutdown requests and configuration reload signals.

## Definition

```c
static void
ProcessSlotSyncInterrupts(WalReceiverConn *wrconn)
```
## Detailed Description
This function serves as the interrupt handler for the replication slot synchronization worker's main loop. It processes two types of interrupts: shutdown requests (SIGINT) and configuration reload requests. When a shutdown request is pending, the function logs the shutdown message and exits the process cleanly. When a configuration reload is pending, it triggers a configuration reread through slotsync_reread_config().

The function starts by calling CHECK_FOR_INTERRUPTS() macro to check for any pending interrupts, then handles specific interrupt types based on global flags.

## Parameters / Member Variables
- `*wrconn`: A pointer to WalReceiverConn structure representing the WAL receiver connection (parameter is accepted but not used in the current implementation)
## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS (macro)
  - ereport
  - [proc_exit](../p/proc_exit.md)
  - [slotsync_reread_config](../s/slotsync_reread_config.md)
- Called from (representative examples):
  - [ReplSlotSyncWorkerMain](../R/ReplSlotSyncWorkerMain.md) (in src/backend/replication/logical/slotsync.c:1489)

## Notes and Other Information
- This is a static function, meaning it's only visible within the slotsync.c compilation unit
- The function handles graceful shutdown by logging the shutdown reason before exiting
- Configuration changes are applied immediately when ConfigReloadPending is set
- The wrconn parameter is currently unused but may be reserved for future functionality

## Simplified Source

```c
static void ProcessSlotSyncInterrupts(WalReceiverConn *wrconn)
{
    // Check for any pending interrupts
    CHECK_FOR_INTERRUPTS();

    // Handle shutdown request
    if (ShutdownRequestPending)
    {
        ereport(LOG,
                errmsg("replication slot synchronization worker is shutting down on receiving SIGINT"));
        proc_exit(0);
    }

    // Handle configuration reload request
    if (ConfigReloadPending)
        slotsync_reread_config();
}
```