# CheckSlotPermissions

## Location
[src/backend/replication/slot.c:1384-1400](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L1384-L1400)

## Overview
Verifies that the current user has the necessary REPLICATION privilege to use replication slots.

## Definition

```c
void
CheckSlotPermissions(void)
```
## Detailed Description
This function performs a security check to ensure that only users with the REPLICATION attribute can access replication slot functionality. It calls  to check if the current user (obtained via ) has replication privileges. If the user lacks the REPLICATION attribute, the function raises an ERROR with appropriate error code and message, preventing unauthorized access to replication slots.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [has_rolreplication](../h/has_rolreplication.md)
  - [GetUserId](../G/GetUserId.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [errdetail](../e/errdetail.md)
- Called from (representative examples):
  - [pg_logical_slot_get_changes_guts](../p/pg_logical_slot_get_changes_guts.md)
  - [pg_create_physical_replication_slot](../p/pg_create_physical_replication_slot.md)
  - [pg_create_logical_replication_slot](../p/pg_create_logical_replication_slot.md)
  - [pg_drop_replication_slot](../p/pg_drop_replication_slot.md)
  - [pg_replication_slot_advance](../p/pg_replication_slot_advance.md)
  - [copy_replication_slot](../c/copy_replication_slot.md)
  - [pg_sync_replication_slots](../p/pg_sync_replication_slots.md)

## Notes and Other Information
This function serves as a security gate for all replication slot operations, ensuring that only privileged users can create, modify, or access replication slots. The REPLICATION attribute is a database role attribute that must be explicitly granted to users who need to perform replication-related operations.