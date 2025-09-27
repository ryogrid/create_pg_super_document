# has_rolreplication

## Location
[src/backend/utils/init/miscinit.c:734-755](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L734-L755)

## Overview
Checks whether a specified role has explicit REPLICATION privilege by examining the pg_authid system catalog.

## Definition

```c
bool
has_rolreplication(Oid roleid)
```
## Detailed Description
This function determines if a given role has replication privileges in PostgreSQL. It first checks if the role is a superuser (which automatically grants all privileges including replication). If not a superuser, it queries the pg_authid system catalog to check the rolreplication attribute of the specified role. The function follows PostgreSQL's standard privilege checking pattern with superuser bypass and system catalog lookup.

## Parameters / Member Variables
- : The object identifier (Oid) of the role to check for replication privileges

## Dependencies
- Functions called/Symbols referenced:
  - [superuser_arg](../s/superuser_arg.md) (checks if role is a superuser)
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

## Simplified Source

```c
// Simplified version of has_rolreplication
bool has_rolreplication(Oid roleid) {
    bool result = false;
    HeapTuple utup;

    // Superusers have all privileges including replication
    if (superuser_arg(roleid)) {
        return true;
    }

    // Look up the role in pg_authid catalog
    utup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
    if (HeapTupleIsValid(utup)) {
        // Check the rolreplication attribute
        result = ((Form_pg_authid) GETSTRUCT(utup))->rolreplication;
        ReleaseSysCache(utup);
    }

    return result;
}
```

Key simplifications made:
- This function is already quite simple, so minimal simplification was needed
- Added descriptive comments explaining the superuser bypass and catalog lookup logic
- Maintained the exact same logic structure including proper cache management
- The function follows a standard PostgreSQL privilege checking pattern that cannot be simplified further
- Preserved the essential security check flow: superuser bypass → catalog lookup → attribute check