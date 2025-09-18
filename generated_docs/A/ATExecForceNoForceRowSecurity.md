# ATExecForceNoForceRowSecurity

## Location
[src/backend/commands/tablecmds.c:16898-16926](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L16898-L16926)

## Overview
Controls whether row-level security policies are enforced for table owners and superusers by updating the relforcerowsecurity field in the pg_class catalog.

## Definition
```c
static void ATExecForceNoForceRowSecurity(Relation rel, bool force_rls)
```

## Detailed Description
ATExecForceNoForceRowSecurity implements the `ALTER TABLE FORCE/NO FORCE ROW LEVEL SECURITY` SQL command functionality. This function controls a specific aspect of row-level security behavior that determines whether RLS policies apply to table owners and superusers.

By default, row-level security policies do not apply to table owners and superusers, allowing them to bypass RLS restrictions. The "FORCE ROW LEVEL SECURITY" option changes this behavior:

- **FORCE ROW LEVEL SECURITY (force_rls=true)**: RLS policies apply to all users, including table owners and superusers
- **NO FORCE ROW LEVEL SECURITY (force_rls=false)**: RLS policies do not apply to table owners and superusers (default behavior)

The function performs a simple catalog update:

1. Retrieves the relation's entry from the pg_class system catalog
2. Updates the relforcerowsecurity boolean field
3. Commits the change to the catalog
4. Triggers post-alter hooks for proper event notification
5. Cleans up allocated memory

This setting only has an effect when row-level security is also enabled for the table.

## Parameters / Member Variables
- `rel`: The relation (table) for which to set the force row security option
- `force_rls`: Boolean flag - true to force RLS for all users, false to allow owners/superusers to bypass

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheCopy1: Retrieves a copy of the relation's pg_class catalog entry
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates the modified tuple in the pg_class catalog
  - InvokeObjectPostAlterHook: Triggers post-alter event hooks
  - [heap_freetuple](../h/heap_freetuple.md): Frees the heap tuple memory
  - Form_pg_class: Structure representing pg_class catalog entries

- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md): Main ALTER TABLE command execution dispatcher

## Notes and Other Information
- This setting is independent of the basic RLS enable/disable setting (relrowsecurity)
- FORCE ROW LEVEL SECURITY only has effect when RLS is also enabled for the table
- The function assumes the caller has appropriate permissions and locks on the relation
- Uses RowExclusiveLock when modifying the pg_class catalog for transactional safety
- Post-alter hooks ensure proper event propagation for monitoring and replication systems
- Error handling includes cache lookup failure detection with appropriate error messages
- The operation is transactionally safe and will be rolled back if the transaction fails
- This feature provides enhanced security by ensuring even privileged users are subject to row security policies when needed