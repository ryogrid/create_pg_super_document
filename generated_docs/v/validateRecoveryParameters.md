# validateRecoveryParameters

## Location
src/backend/access/transam/xlogrecovery.c: 1109 - 1207

## Overview
Validates and normalizes recovery configuration parameters for PostgreSQL WAL recovery, ensuring required parameters are present and resolving inconsistencies in recovery target settings.

## Definition


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
  - DatumGetTimestampTz (datum conversion utility)
  - [CStringGetDatum](../C/CStringGetDatum.md) (datum conversion utility)
- Called from:
  - [InitWalRecovery](../I/InitWalRecovery.md) (during WAL recovery initialization)

## Notes and Other Information
- Only executes when ArchiveRecoveryRequested is true
- Issues WARNING (not FATAL) when standby mode lacks both primary_conninfo and restore_command
- Automatically changes RECOVERY_TARGET_ACTION_PAUSE to RECOVERY_TARGET_ACTION_SHUTDOWN when hot standby is disabled
- Timeline 1 is special-cased as it doesn't require a history file
- The function modifies global variables like recoveryTargetAction, recoveryTargetTime, and recoveryTargetTLI based on validation results