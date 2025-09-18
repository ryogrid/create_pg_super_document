# TargetPrivilegesCheck

## Location
src/backend/replication/logical/worker.c: 2341 - 2372

## Overview
Verifies that the subscription owner has sufficient privileges to perform replication operations on target relations and ensures row-level security is not enabled on the target.

## Definition
```c
static void TargetPrivilegesCheck(Relation rel, AclMode mode)
```

## Detailed Description
This function performs privilege validation for logical replication operations on target relations. It serves as a security checkpoint that ensures the subscription owner has the necessary permissions to perform the requested operation (INSERT, UPDATE, DELETE, or TRUNCATE) on the target table. The function performs two critical checks:

1. **Privilege Verification**: Uses the PostgreSQL ACL system to verify that the current user (subscription owner) has the required privileges on the target relation for the specified operation mode.

2. **Row-Level Security Check**: Explicitly prohibits replication to tables that have row-level security (RLS) enabled, as the logical replication infrastructure lacks the necessary components to properly honor RLS policies. This restriction applies to all operations, including TRUNCATE, to maintain consistency in the replication behavior.

The function raises appropriate errors if either check fails, preventing potentially unauthorized or inconsistent replication operations.

## Parameters / Member Variables
- `rel`: The target Relation structure representing the table being replicated to
- `mode`: AclMode specifying the type of privilege required (e.g., ACL_INSERT, ACL_UPDATE, ACL_DELETE, ACL_TRUNCATE)

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetRelid
  - [GetUserId](../G/GetUserId.md)
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [get_relkind_objtype](../g/get_relkind_objtype.md)
  - [get_rel_name](../g/get_rel_name.md)
  - [check_enable_rls](../c/check_enable_rls.md)
  - [GetUserNameFromId](../G/GetUserNameFromId.md)
  - RelationGetRelationName
  - AclResult (data type)
  - RLS_ENABLED (constant)
- Called from (representative examples):
  - [apply_handle_insert_internal](../a/apply_handle_insert_internal.md)
  - [apply_handle_update_internal](../a/apply_handle_update_internal.md)
  - [apply_handle_delete_internal](../a/apply_handle_delete_internal.md)
  - [FindReplTupleInLocalRel](../F/FindReplTupleInLocalRel.md)
  - [apply_handle_tuple_routing](../a/apply_handle_tuple_routing.md)
  - [apply_handle_truncate](../a/apply_handle_truncate.md)

## Notes and Other Information
- This is a static function within the logical replication worker module
- RLS restriction exists because logical replication workers lack infrastructure to properly evaluate RLS policies
- The RLS prohibition extends to all operations, including TRUNCATE, to maintain behavioral consistency
- Essential security component that prevents unauthorized replication operations
- Part of PostgreSQL's logical replication privilege enforcement mechanism