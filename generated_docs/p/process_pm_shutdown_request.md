# process_pm_shutdown_request

## Location
[src/backend/postmaster/postmaster.c:2193-2343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L2193-L2343)

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

## Simplified Source

```c
// Simplified version of process_pm_shutdown_request
static void process_pm_shutdown_request(void) {
    int mode;

    // Log shutdown request received
    ereport(DEBUG2, (errmsg_internal("postmaster received shutdown request signal")));
    pending_pm_shutdown_request = false;

    // Determine shutdown mode - immediate takes highest priority
    if (pending_pm_immediate_shutdown_request) {
        pending_pm_immediate_shutdown_request = false;
        pending_pm_fast_shutdown_request = false;
        mode = ImmediateShutdown;
    } else if (pending_pm_fast_shutdown_request) {
        pending_pm_fast_shutdown_request = false;
        mode = FastShutdown;
    } else {
        mode = SmartShutdown;
    }

    switch (mode) {
        case SmartShutdown:
            // Wait for children to finish their work naturally
            if (Shutdown >= SmartShutdown) break;

            Shutdown = SmartShutdown;
            ereport(LOG, (errmsg("received smart shutdown request")));

            // Update status in lock file and notify systemd
            AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STOPPING);

            // Set state based on current postmaster state
            if (pmState == PM_RUN || pmState == PM_HOT_STANDBY) {
                connsAllowed = false;  // Stop accepting new connections
            } else if (pmState == PM_STARTUP || pmState == PM_RECOVERY) {
                pmState = PM_STOP_BACKENDS;  // Move to backend stopping
            }

            PostmasterStateMachine();  // Advance state machine
            break;

        case FastShutdown:
            // Abort active transactions and force client disconnections
            if (Shutdown >= FastShutdown) break;

            Shutdown = FastShutdown;
            ereport(LOG, (errmsg("received fast shutdown request")));

            // Update status in lock file and notify systemd
            AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STOPPING);

            // Transition state based on current mode
            if (pmState == PM_STARTUP || pmState == PM_RECOVERY) {
                pmState = PM_STOP_BACKENDS;
            } else if (pmState == PM_RUN || pmState == PM_HOT_STANDBY) {
                ereport(LOG, (errmsg("aborting any active transactions")));
                pmState = PM_STOP_BACKENDS;
            }

            PostmasterStateMachine();  // Let state machine handle termination
            break;

        case ImmediateShutdown:
            // Kill all processes immediately without cleanup
            if (Shutdown >= ImmediateShutdown) break;

            Shutdown = ImmediateShutdown;
            ereport(LOG, (errmsg("received immediate shutdown request")));

            // Update status in lock file and notify systemd
            AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STOPPING);

            // Send SIGQUIT to all children immediately
            SetQuitSignalReason(PMQUIT_FOR_STOP);
            TerminateChildren(SIGQUIT);
            pmState = PM_WAIT_BACKENDS;

            // Start timer for forced termination
            AbortStartTime = time(NULL);

            PostmasterStateMachine();  // Wait for processes to exit
            break;
    }
}
```

Key simplifications made:
- Removed platform-specific systemd notification code blocks for clarity
- Consolidated similar state transition logic between FastShutdown and SmartShutdown
- Simplified conditional checks while preserving essential logic flow
- Added brief explanatory comments for each shutdown mode's strategy
- Removed detailed internal comments that don't affect the core algorithm
- Focused on the main execution path and decision logic