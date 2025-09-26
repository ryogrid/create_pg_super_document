# vacuum_is_permitted_for_relation

## Location
[src/backend/commands/vacuum.c:717-768](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L717-L768)

## Overview
Checks if the current user has sufficient privileges to vacuum or analyze a specific relation, issuing warnings and returning false if permission is denied.

## Definition

```c
bool
vacuum_is_permitted_for_relation(Oid relid, Form_pg_class reltuple,
								 bits32 options)
```
## Detailed Description
This function implements PostgreSQL's privilege checking mechanism for VACUUM and ANALYZE operations on individual relations. It determines whether the current user has the necessary permissions to perform the requested operation(s) on a specific table or relation.

The function implements a two-tier privilege model:
1. Database ownership: Users who own the current database can vacuum/analyze any non-shared relation within that database
2. MAINTAIN privilege: Users with explicit MAINTAIN privilege on the relation can perform these operations regardless of database ownership

If the user lacks sufficient privileges, the function issues appropriate WARNING messages indicating permission denial and the relation being skipped. This allows vacuum operations to continue processing other relations rather than failing entirely.

For combined VACUUM ANALYZE operations, the function prioritizes VACUUM permission checking and will only log the VACUUM warning to avoid duplicate messages.

## Parameters / Member Variables
- : Object identifier (OID) of the relation to check permissions for
- : Form_pg_class structure containing the relation's catalog information, particularly the relation name and shared status
- : Bitfield indicating which operations are requested (VACOPT_VACUUM, VACOPT_ANALYZE, or both)

## Dependencies
- Functions called/Symbols referenced:
  - object_ownercheck (checks database ownership)
  - pg_class_aclcheck (checks ACL permissions)
  - GetUserId (current user identification)
  - ereport (warning message generation)
  - ACL_MAINTAIN (privilege constant)
- Called from (representative examples):
  - vacuum_rel (per-relation vacuum processing)
  - analyze_rel (per-relation analysis processing)
  - expand_vacuum_rel (relation expansion utility)
  - get_all_vacuum_rels (database-wide relation discovery)

## Notes and Other Information
- Returns true if permission is granted, false if denied (allowing caller to skip the relation)
- Shared relations (system catalogs) require database ownership from the template0/template1 context, not local database ownership
- The MAINTAIN privilege was introduced to allow non-owners to perform maintenance operations on specific relations
- Warning messages are localized and include the relation name for user clarity
- For VACUUM ANALYZE operations, only the VACUUM permission warning is shown to prevent log spam
- Used throughout the vacuum subsystem as a centralized authorization check