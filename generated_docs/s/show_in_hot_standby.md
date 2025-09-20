# show_in_hot_standby

## Location
[src/backend/access/transam/xlog.c:4777-4800](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L4777-L4800)

## Overview
A GUC show hook function that returns the current hot standby status as a string representation for display purposes.

## Definition

```c
const char *
show_in_hot_standby(void)
```
## Detailed Description
This function serves as a show hook for the PostgreSQL GUC (Grand Unified Configuration) system to display the current hot standby status. Unlike many GUC variables that simply return their stored values, this function dynamically queries the actual recovery state from shared memory to provide real-time status information. This ensures that the displayed value reflects the current state even if examined during query execution (intra-query). The function returns "on" if the server is currently in recovery mode (hot standby), and "off" otherwise.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
- Called from (representative examples):
  - GUC system (referenced in guc_hooks.h)

## Notes and Other Information
- This function provides up-to-date state information by checking shared memory rather than relying on a cached GUC variable
- The underlying GUC variable (in_hot_standby_guc) only changes when transmitting new values to clients
- Used specifically for displaying the in_hot_standby GUC parameter value
- Located in src/backend/access/transam/xlog.c:4777-4800