# GetActiveWalLevelOnStandby

## Location
[src/backend/access/transam/xlog.c:4814-4822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L4814-L4822)

## Overview
Retrieves the active WAL level from the control file, particularly useful for standby servers where the effective WAL level may differ from the originally configured value.

## Definition
```c
WalLevel GetActiveWalLevelOnStandby(void)
```

## Detailed Description
This function returns the WAL level stored in the control file, which represents the currently active WAL level. This is particularly important for standby servers because the effective WAL level on a standby may be different from what was originally configured on that standby server. The standby inherits the WAL level from the primary server through the control file, and this function provides access to that inherited value rather than the standby's local configuration.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - ControlFile (global variable access)
- Called from (representative examples):
  - [CheckLogicalDecodingRequirements](../C/CheckLogicalDecodingRequirements.md)
  - [WALAvailability](../W/WALAvailability.md) (header reference)

## Notes and Other Information
- Essential for standby servers to determine their effective WAL level
- The WAL level on standby is determined by the primary server, not local configuration
- Used in logical replication requirements checking
- Returns WalLevel enum type
- Located in src/backend/access/transam/xlog.c:4814-4822

## Simplified Source

```c
// Simplified version of GetActiveWalLevelOnStandby
WalLevel GetActiveWalLevelOnStandby(void) {
    // Return the WAL level from the control file
    return ControlFile->wal_level;
}
```

Key simplifications made:
- Function is already very simple, just returns control file WAL level
- Essential for standby servers to get effective WAL level from primary