# slotsync_reread_config

## Location
[src/backend/replication/logical/slotsync.c:1106-1154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/slotsync.c#L1106-L1154)

## Overview
Handles configuration file reloading for the slot synchronization worker process, exiting appropriately when critical parameters change.

## Definition

```c
static void
slotsync_reread_config(void)
```
## Detailed Description
This function manages configuration changes for PostgreSQL's replication slot synchronization worker process. It implements a configuration reload mechanism that responds to SIGHUP signals by re-reading the configuration file and determining whether the worker process should continue running or exit.

The function performs the following operations:
1. **Backup current settings**: Stores current values of critical configuration parameters before reload
2. **Reload configuration**: Processes the configuration file for SIGHUP context changes
3. **Detect changes**: Compares old and new values of key slot synchronization parameters
4. **Handle shutdowns**: Exits cleanly if  is disabled
5. **Handle restarts**: Exits with restart signal if connection parameters change

The function distinguishes between two types of configuration changes:
- **Shutdown scenario**: When  is disabled, the worker logs and exits permanently
- **Restart scenario**: When connection parameters (, ) or  change, the worker exits to allow the postmaster to restart it with new settings

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  -  - Creates string duplicates for comparison
  -  - Reloads configuration file with PGC_SIGHUP context
  -  - Terminates the worker process
  -  - Configuration context for signal-based reloads
  - Global configuration variables: , , , 
  -  - Global slot synchronization context
- Called from:
  -  - Signal handling function for slot sync worker (line 1168)

## Notes and Other Information
- Static function - internal to slotsync.c
- Manages the ConfigReloadPending flag to handle SIGHUP signals
- Resets  to 0 when requesting restart to bypass normal restart intervals
- Provides clear logging for both shutdown and restart scenarios
- Part of PostgreSQL's graceful configuration management for background worker processes
- Critical for maintaining slot synchronization worker stability during configuration changes
- Uses string comparison to detect connection parameter changes accurately