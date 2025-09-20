# ValidateSlotSyncParams

## Location
[src/backend/replication/logical/slotsync.c:1039-1105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/slotsync.c#L1039-L1105)

## Overview
Validates that all necessary PostgreSQL configuration parameters (GUCs) are properly set for replication slot synchronization to function correctly.

## Definition

```c
bool
ValidateSlotSyncParams(int elevel)
```
## Detailed Description
This function performs comprehensive validation of PostgreSQL configuration settings required for logical replication slot synchronization between primary and standby servers. It checks four critical configuration parameters and reports errors at the specified error level if any are missing or incorrectly set.

The function validates the following requirements:
1. **WAL Level**: Ensures  is set to 'logical' or higher, which is necessary for logical replication operations
2. **Primary Slot**: Verifies that  is configured to ensure row retention on the primary server
3. **Hot Standby Feedback**: Confirms  is enabled to coordinate transaction visibility between primary and standby
4. **Connection Info**: Validates that  is set to enable communication with the primary server

Each validation failure results in an error report with a descriptive message indicating which parameter needs to be corrected.

## Parameters / Member Variables
- : Error level to use when reporting validation failures (e.g., ERROR, WARNING, LOG)

## Dependencies
- Functions called/Symbols referenced:
  -  - Constant defining the minimum required WAL level
  - Global configuration variables: , , , 
- Called from:
  -  - Postmaster function to conditionally start sync worker (src/backend/postmaster/postmaster.c:4095)
  -  - SQL function for manual slot synchronization (src/backend/replication/slotfuncs.c:905)
  - Referenced in  header file (src/include/replication/slotsync.h:27)

## Notes and Other Information
- Returns  if all validations pass,  if any validation fails
- Provides clear, translatable error messages for each configuration issue
- Essential gate-keeping function that prevents slot synchronization from starting with invalid configurations
- Used both at startup (via postmaster) and on-demand (via SQL functions)
- Part of PostgreSQL's configuration validation framework for logical replication features
- The elevel parameter allows flexible error handling depending on the calling context