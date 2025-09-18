# process_pm_shutdown_request

## Location
src/backend/postmaster/postmaster.c: 2193 - 2343

## Overview
Processes shutdown requests received by the postmaster, determining the shutdown mode and transitioning the postmaster state to initiate the appropriate shutdown sequence.

## Definition
```c
static void process_pm_shutdown_request(void)
```

## Detailed Description
This function is the main handler for processing shutdown requests after signals have been received and flags have been set by the signal handler. It implements three different shutdown modes:

1. **Smart Shutdown (SIGTERM)**: Waits for existing client connections to complete normally before shutting down
2. **Fast Shutdown (SIGINT)**: Terminates active transactions and forces client disconnections, but performs proper cleanup
3. **Immediate Shutdown (SIGQUIT)**: Kills all processes immediately without cleanup, used for emergency situations

The function prioritizes shutdown requests with immediate shutdown taking precedence over fast shutdown, which takes precedence over smart shutdown. It updates the postmaster state machine and coordinates with systemd when available.

## Parameters / Member Variables
This function takes no parameters but operates on several global state variables:
- `pending_pm_shutdown_request`: General shutdown flag
- `pending_pm_fast_shutdown_request`: Fast shutdown flag  
- `pending_pm_immediate_shutdown_request`: Immediate shutdown flag
- `pmState`: Current postmaster state
- `Shutdown`: Current shutdown mode

## Dependencies
- Functions called/Symbols referenced:
  - `ereport` - Logging functionality
  - [AddToDataDirLockFile](../A/AddToDataDirLockFile.md) - Updates lock file with status
  - [PostmasterStateMachine](../P/PostmasterStateMachine.md) - Advances state machine
  - [SetQuitSignalReason](../S/SetQuitSignalReason.md) - Sets reason for quit signals
  - [TerminateChildren](../T/TerminateChildren.md) - Sends termination signals to child processes
  - `time` - Gets current time for abort timeout
- Called from (representative examples):
  - [ServerLoop](../S/ServerLoop.md) - Main postmaster event loop

## Notes and Other Information
- The function handles multiple concurrent shutdown requests by prioritizing the most severe (immediate > fast > smart)
- Integrates with systemd service management when compiled with USE_SYSTEMD
- Updates the postmaster lock file to indicate stopping status
- For immediate shutdown, sets a timer to track how long child processes take to exit
- The actual termination of child processes and cleanup is handled by the PostmasterStateMachine function
- Each shutdown mode follows a different strategy for process termination and cleanup