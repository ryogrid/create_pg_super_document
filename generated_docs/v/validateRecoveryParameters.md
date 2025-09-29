# validateRecoveryParameters

## Location
[src/backend/access/transam/xlogrecovery.c:1109-1207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogrecovery.c#L1109-L1207)

## Overview
Validates and normalizes recovery configuration parameters for PostgreSQL WAL recovery, ensuring required parameters are present and resolving inconsistencies in recovery target settings.

## Definition

```c
static void
validateRecoveryParameters(void)
```
## Detailed Description
This function performs comprehensive validation of recovery parameters during WAL recovery initialization. It checks that all required recovery parameters are properly configured based on the recovery mode (standby vs archive recovery), resolves parameter conflicts, and computes final values for recovery targets.

The function handles several key validation tasks:
- Ensures required connection parameters are set for standby mode
- Validates that restore_command is specified when not in standby mode  
- Resolves conflicts between recovery target actions and hot standby settings
- Parses and validates recovery target time strings
- Validates and computes recovery target timeline values

## Parameters / Member Variables
This function takes no parameters and operates on global recovery configuration variables.

## Dependencies
- Functions called/Symbols referenced:
  - [existsTimeLineHistory](../e/existsTimeLineHistory.md) (checks if timeline history file exists)
  - [findNewestTimeLine](../f/findNewestTimeLine.md) (finds the newest available timeline)
  - [timestamptz_in](../t/timestamptz_in.md) (parses timestamp strings)
  - DirectFunctionCall3 (PostgreSQL function call interface)
  - [DatumGetTimestampTz](../D/DatumGetTimestampTz.md) (datum conversion utility)
  - [CStringGetDatum](../C/CStringGetDatum.md) (datum conversion utility)
- Called from:
  - [InitWalRecovery](../I/InitWalRecovery.md) (during WAL recovery initialization)

## Notes and Other Information
- Only executes when ArchiveRecoveryRequested is true
- Issues WARNING (not FATAL) when standby mode lacks both primary_conninfo and restore_command
- Automatically changes RECOVERY_TARGET_ACTION_PAUSE to RECOVERY_TARGET_ACTION_SHUTDOWN when hot standby is disabled
- Timeline 1 is special-cased as it doesn't require a history file
- The function modifies global variables like recoveryTargetAction, recoveryTargetTime, and recoveryTargetTLI based on validation results

## Simplified Source

```c
// Simplified version of validateRecoveryParameters
static void validateRecoveryParameters(void) {
    // Exit early if archive recovery is not requested
    if (!ArchiveRecoveryRequested)
        return;

    // Validate compulsory parameters based on standby mode
    if (StandbyModeRequested) {
        // In standby mode: warn if both primary_conninfo and restore_command are missing
        if (no_primary_connection_info && no_restore_command) {
            ereport(WARNING, "specify either primary_conninfo or restore_command");
        }
    } else {
        // In archive recovery mode: restore_command is mandatory
        if (no_restore_command) {
            ereport(FATAL, "must specify restore_command when standby mode is not enabled");
        }
    }

    // Override inconsistent recovery target action settings
    if (recovery_target_is_pause && hot_standby_disabled) {
        recoveryTargetAction = RECOVERY_TARGET_ACTION_SHUTDOWN;
    }

    // Parse recovery target time if specified
    if (recoveryTarget == RECOVERY_TARGET_TIME) {
        recoveryTargetTime = parse_timestamp_string(recovery_target_time_string);
    }

    // Validate and set recovery target timeline
    if (recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_NUMERIC) {
        TimeLineID target_timeline = recoveryTargetTLIRequested;

        // Verify timeline exists (timeline 1 is always valid)
        if (target_timeline != 1 && !existsTimeLineHistory(target_timeline)) {
            ereport(FATAL, "recovery target timeline %u does not exist", target_timeline);
        }
        recoveryTargetTLI = target_timeline;

    } else if (recoveryTargetTimeLineGoal == RECOVERY_TARGET_TIMELINE_LATEST) {
        // Find the newest available timeline
        recoveryTargetTLI = findNewestTimeLine(recoveryTargetTLI);

    } else {
        // Use timeline from control file (default case)
        // recoveryTargetTLI already set from ControlFile
    }
}
```

Key simplifications made:
- Simplified complex string comparison conditions into readable boolean expressions
- Abstracted detailed DirectFunctionCall3 timestamp parsing into conceptual parse_timestamp_string()
- Removed verbose error message formatting for clarity
- Consolidated parameter validation logic into clear conditional blocks
- Added descriptive comments explaining the purpose of each validation step
- Simplified timeline validation logic while preserving the core algorithm