# CompleteCommitTsInitialization

## Location
src/backend/access/transam/commit_ts.c: 642 - 663

## Overview
CompleteCommitTsInitialization finalizes the commit timestamp subsystem initialization after recovery has finished, either activating or deactivating the feature based on the current configuration.

## Definition
```c
void CompleteCommitTsInitialization(void)
```

## Detailed Description
This function serves as the final step in the commit timestamp subsystem initialization process during database startup. It must be called exactly once after recovery has completed, and it makes the definitive decision about whether commit timestamp tracking should be active or inactive based on the track_commit_timestamp configuration parameter.

The function handles both activation and deactivation scenarios:
- If track_commit_timestamp is disabled, it deactivates the subsystem and removes any leftover commit timestamp data
- If track_commit_timestamp is enabled, it activates the subsystem for normal operation

This dual-mode operation is essential for both primary and standby servers, as the activation state depends on control file contents and parameter changes that may have been replayed during recovery.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [DeactivateCommitTs](../D/DeactivateCommitTs.md)
  - [ActivateCommitTs](../A/ActivateCommitTs.md)
  - track_commit_timestamp (global configuration variable)
- Called from (representative examples):
  - [StartupXLOG](../S/StartupXLOG.md)

## Notes and Other Information
- Must be called exactly ONCE during startup, specifically after recovery completion
- Critical timing requirement: must be called after recovery has finished but as part of the startup sequence
- The function checks the global track_commit_timestamp configuration parameter to determine the appropriate action
- Handles cleanup of leftover data when the feature is disabled
- Works for both primary and standby servers
- The activation/deactivation decision can be influenced by XLOG_PARAMETER_CHANGE records replayed during recovery
- Declared in src/include/access/commit_ts.h for external access