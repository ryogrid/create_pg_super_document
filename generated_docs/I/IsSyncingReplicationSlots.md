# IsSyncingReplicationSlots

## Location
src/backend/replication/logical/slotsync.c: 1650 - 1658

## Overview
Checks whether the current process is performing replication slot synchronization, either as a slot sync worker or backend executing SQL functions.

## Definition


## Detailed Description
This function provides a simple way to determine if the current process is actively synchronizing replication slots from a primary server to a standby. It returns the value of the static  flag, which is set to true only when the current process is performing slot synchronization operations.

The function distinguishes between the process-local  flag and the shared memory  flag. While the shared memory flag prevents concurrent slot syncs across different processes, this function specifically indicates whether *this* process is currently engaged in slot synchronization.

This check is useful for various components that need to modify their behavior when slot synchronization is in progress, such as restricting certain operations during sync or applying different validation rules.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - syncing_slots (static variable)
- Called from (representative examples):
  - CreateDecodingContext
  - ReplicationSlotCreate
  - GetStandbyFlushRecPtr

## Notes and Other Information
- This function can be called by both slot sync workers and backend processes executing the  SQL function
- The underlying  variable is a process-local static boolean that gets set during slot synchronization operations
- Used for validation and behavioral changes in replication-related code paths
- Located in src/backend/replication/logical/slotsync.c:1644-1653