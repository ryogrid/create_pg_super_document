# has_rolreplication

## Location
[src/backend/utils/init/miscinit.c:734-755](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L734-L755)

## Overview
Checks whether a specified role has explicit REPLICATION privilege by examining the pg_authid system catalog.

## Definition


## Detailed Description
This function determines if a given role has replication privileges in PostgreSQL. It first checks if the role is a superuser (which automatically grants all privileges including replication). If not a superuser, it queries the pg_authid system catalog to check the rolreplication attribute of the specified role. The function follows PostgreSQL's standard privilege checking pattern with superuser bypass and system catalog lookup.

## Parameters / Member Variables
- : The object identifier (Oid) of the role to check for replication privileges

## Dependencies
- Functions called/Symbols referenced:
  - superuser_arg (checks if role is a superuser)
  - Form_pg_authid (structure representing pg_authid catalog entries)
  - [SearchSysCache1](../S/SearchSysCache1.md), HeapTupleIsValid, ReleaseSysCache (system catalog access functions)
  - AUTHOID, ObjectIdGetDatum, GETSTRUCT (catalog access macros)
- Called from (representative examples):
  - [CreateRole](../C/CreateRole.md) (src/backend/commands/user.c:333)
  - [AlterRole](../A/AlterRole.md) (src/backend/commands/user.c:806)
  - [CheckSlotPermissions](../C/CheckSlotPermissions.md) (src/backend/replication/slot.c:1386)
  - [binary_upgrade_logical_slot_has_caught_up](../b/binary_upgrade_logical_slot_has_caught_up.md) (src/backend/utils/adt/pg_upgrade_support.c:297)
  - [InitPostgres](../I/InitPostgres.md) (src/backend/utils/init/postinit.c:977)
  - INIT_PG_OVERRIDE_ROLE_LOGIN (src/include/miscadmin.h:515)

## Notes and Other Information
- Returns true immediately for superusers, bypassing explicit privilege checks
- Uses system cache (syscache) for efficient catalog lookups
- Part of PostgreSQL's role-based access control for replication operations
- Essential for logical replication slot management and streaming replication security
- Used during database initialization and role management operations