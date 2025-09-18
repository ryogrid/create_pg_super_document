# StartupProcTriggerHandler

## Location
src/backend/postmaster/startup.c: 93 - 100

## Overview
A SIGUSR2 signal handler that triggers the promotion of a standby server to primary by setting a flag to finish recovery operations.

## Definition
static void StartupProcTriggerHandler(SIGNAL_ARGS)

## Detailed Description
StartupProcTriggerHandler is a signal handler function specifically designed to handle SIGUSR2 signals sent to the startup process during PostgreSQL recovery. When invoked, it initiates the promotion process by setting the promote_signaled flag to true and calling WakeupRecovery() to wake up the recovery process. This mechanism is essential for PostgreSQL's streaming replication and hot standby functionality, allowing a standby server to be promoted to primary status on demand.

The function operates asynchronously - it simply sets a flag and wakes up the recovery process rather than performing the promotion directly, ensuring signal handler safety and proper coordination with the main recovery loop.

## Parameters / Member Variables
- : Standard PostgreSQL signal handler arguments macro (typically expands to int signum for signal number)

## Dependencies
- Functions called/Symbols referenced:
  - [WakeupRecovery](../W/WakeupRecovery.md) (wakes up the recovery process)
  - SIGNAL_ARGS (signal handler arguments macro)
- Called from (representative examples):
  - [StartupProcessMain](StartupProcessMain.md) (registers this as SIGUSR2 handler)

## Notes and Other Information
- This handler is registered specifically for SIGUSR2 signals during startup process initialization
- Sets the global promote_signaled flag which is checked by the main recovery loop
- Part of PostgreSQL's promotion mechanism for converting standby servers to primary
- Must be signal-safe and minimal in its operations to avoid race conditions
- The actual promotion work is handled asynchronously by the main recovery process after being awakened