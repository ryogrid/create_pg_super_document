# CheckSlotPermissions

## Location
src/backend/replication/slot.c: 1384 - 1400

## Overview
Verifies that the current user has the necessary REPLICATION privilege to use replication slots.

## Definition


## Detailed Description
This function performs a security check to ensure that only users with the REPLICATION attribute can access replication slot functionality. It calls  to check if the current user (obtained via ) has replication privileges. If the user lacks the REPLICATION attribute, the function raises an ERROR with appropriate error code and message, preventing unauthorized access to replication slots.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - has_rolreplication
  - GetUserId
  - ereport
  - errcode
  - errmsg
  - errdetail
- Called from (representative examples):
  - pg_logical_slot_get_changes_guts
  - pg_create_physical_replication_slot
  - pg_create_logical_replication_slot
  - pg_drop_replication_slot
  - pg_replication_slot_advance
  - copy_replication_slot
  - pg_sync_replication_slots

## Notes and Other Information
This function serves as a security gate for all replication slot operations, ensuring that only privileged users can create, modify, or access replication slots. The REPLICATION attribute is a database role attribute that must be explicitly granted to users who need to perform replication-related operations.