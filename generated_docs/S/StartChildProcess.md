# StartChildProcess

## Location
[src/backend/postmaster/postmaster.c:3926-3961](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L3926-L3961)

## Overview
StartChildProcess is an auxiliary process launcher in the PostgreSQL postmaster that creates and starts child processes of various types by forking and delegating to postmaster_child_launch.

## Definition


## Detailed Description
StartChildProcess serves as a wrapper function for creating auxiliary child processes in the PostgreSQL postmaster. It takes a BackendType parameter that determines what kind of child process will be started (such as background writer, checkpointer, WAL writer, etc.). The function calls postmaster_child_launch to perform the actual fork operation and handle the low-level process creation details. All child processes initially execute AuxiliaryProcessMain, which handles common initialization tasks.

The function includes error handling for fork failures - if forking fails for a startup process (B_STARTUP), the postmaster exits immediately since this is considered fatal during initialization. For other process types, fork failures are logged but don't cause immediate termination.

## Parameters / Member Variables
- `type`: A BackendType enum value that specifies which kind of auxiliary process to start (B_STARTUP, B_BG_WRITER, B_CHECKPOINTER, B_WAL_WRITER, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [postmaster_child_launch](../p/postmaster_child_launch.md) (performs the actual process forking)
  - [PostmasterChildName](../P/PostmasterChildName.md) (gets human-readable name for error reporting)
  - [ExitPostmaster](../E/ExitPostmaster.md) (terminates postmaster on critical failures)
  - ereport (error reporting system)
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md) (initial process startup)
  - [ServerLoop](ServerLoop.md) (process lifecycle management)
  - [PostmasterStateMachine](../P/PostmasterStateMachine.md) (state-driven process management)
  - [StartAutovacuumWorker](StartAutovacuumWorker.md) (autovacuum worker creation)
  - [MaybeStartWalReceiver](../M/MaybeStartWalReceiver.md) (WAL receiver startup)
  - [MaybeStartWalSummarizer](../M/MaybeStartWalSummarizer.md) (WAL summarizer startup)
  - [MaybeStartSlotSyncWorker](../M/MaybeStartSlotSyncWorker.md) (slot sync worker startup)

## Notes and Other Information
- Returns the child process PID on success, or 0 on failure
- Fork failure for B_STARTUP type processes causes immediate postmaster termination as it's considered fatal during database startup
- All child processes created by this function initially execute AuxiliaryProcessMain for common setup
- This is a static function internal to postmaster.c, serving as a consistent interface for auxiliary process creation