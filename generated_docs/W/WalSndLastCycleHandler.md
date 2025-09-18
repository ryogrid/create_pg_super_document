# WalSndLastCycleHandler

## Location
[src/backend/replication/walsender.c:3624-3631](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L3624-L3631)

## Overview
WalSndLastCycleHandler is a SIGUSR2 signal handler that triggers the final cycle of a WAL sender process before shutdown, setting a flag to indicate the last transmission cycle should begin.

## Definition
```c
static void WalSndLastCycleHandler(SIGNAL_ARGS)
```

## Detailed Description
This function serves as a signal handler for SIGUSR2 that initiates the final phase of WAL sender shutdown. When invoked, it sets the got_SIGUSR2 flag to true and wakes up the main WAL sender loop by setting its latch. This signal is expected to be sent when the WAL sender has already transitioned to WALSNDSTATE_STOPPING state. The function triggers the WAL sender to perform one final transmission cycle to ensure all remaining WAL data is sent before the process terminates.

## Parameters / Member Variables
- Standard signal handler parameters via SIGNAL_ARGS macro (typically signal number and signal info)

## Dependencies
- Functions called/Symbols referenced:
  - SIGNAL_ARGS (macro for signal handler parameters)
  - [SetLatch](../S/SetLatch.md) (function to wake up waiting process)
  - got_SIGUSR2 (global flag variable)
  - MyLatch (global latch for current process)
- Called from (representative examples):
  - [LagTracker](../L/LagTracker.md)
  - [WalSndSignals](WalSndSignals.md)

## Notes and Other Information
- This is a static function, only accessible within the walsender.c module
- Part of the coordinated shutdown sequence for WAL sender processes
- The WAL sender should already be in WALSNDSTATE_STOPPING state when this handler is called
- Uses the PostgreSQL latch mechanism to efficiently wake up the main loop
- The got_SIGUSR2 flag is checked by the main WAL sender loop to initiate final transmission