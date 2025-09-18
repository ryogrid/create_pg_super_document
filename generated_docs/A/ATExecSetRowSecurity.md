# ATExecSetRowSecurity

## Location
src/backend/commands/tablecmds.c: 16868 - 16897

## Overview
Enables or disables row-level security (RLS) for a table by updating the relrowsecurity field in the pg_class catalog.

## Definition
```c
static void ATExecSetRowSecurity(Relation rel, bool rls)
```

## Detailed Description
ATExecSetRowSecurity implements the `ALTER TABLE ENABLE/DISABLE ROW LEVEL SECURITY` SQL command functionality. Row-level security is a PostgreSQL feature that allows fine-grained access control by restricting which rows a user can see or modify based on security policies.

The function performs a straightforward operation:

1. Retrieves the relation's entry from the pg_class system catalog
2. Updates the relrowsecurity boolean field to the specified value
3. Commits the change to the catalog
4. Triggers post-alter hooks for proper event notification
5. Cleans up allocated memory

When row-level security is enabled (rls=true), PostgreSQL will enforce any row security policies defined for the table. When disabled (rls=false), row security policies are ignored and all rows are accessible according to regular table-level permissions.

Note that enabling RLS alone does not restrict access - actual row security policies must be created separately using `CREATE POLICY` commands.

## Parameters / Member Variables
- `rel`: The relation (table) for which to enable or disable row-level security
- `rls`: Boolean flag - true to enable RLS, false to disable RLS

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
- The function assumes the caller has appropriate permissions and locks on the relation
- Row-level security can be enabled on any table, but policies must be created separately to have any effect
- When RLS is disabled, existing policies remain defined but are not enforced
- The function uses RowExclusiveLock when modifying the pg_class catalog
- Post-alter hooks ensure proper event propagation for monitoring and replication systems
- Error handling includes cache lookup failure detection with appropriate error messages
- The operation is transactionally safe and will be rolled back if the transaction fails