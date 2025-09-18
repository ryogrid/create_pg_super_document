# GetActiveWalLevelOnStandby

## Location
src/backend/access/transam/xlog.c: 4814 - 4822

## Overview
Retrieves the active WAL level from the control file, particularly useful for standby servers where the effective WAL level may differ from the originally configured value.

## Definition
```c
WalLevel GetActiveWalLevelOnStandby(void)
```

## Detailed Description
This function returns the WAL level stored in the control file, which represents the currently active WAL level. This is particularly important for standby servers because the effective WAL level on a standby may be different from what was originally configured on that standby server. The standby inherits the WAL level from the primary server through the control file, and this function provides access to that inherited value rather than the standby's local configuration.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - ControlFile (global variable access)
- Called from (representative examples):
  - CheckLogicalDecodingRequirements
  - WALAvailability (header reference)

## Notes and Other Information
- Essential for standby servers to determine their effective WAL level
- The WAL level on standby is determined by the primary server, not local configuration
- Used in logical replication requirements checking
- Returns WalLevel enum type
- Located in src/backend/access/transam/xlog.c:4814-4822