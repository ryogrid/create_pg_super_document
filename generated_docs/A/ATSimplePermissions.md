# ATSimplePermissions

## Location
[src/backend/commands/tablecmds.c:6543-6617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L6543-L6617)

## Overview
A core permission validation function that ensures the current user has the necessary rights to perform ALTER TABLE operations on a relation, including target type validation, ownership checks, and system catalog protection.

## Definition

```c
static void
ATSimplePermissions(AlterTableType cmdtype, Relation rel, int allowed_targets)
```
## Detailed Description
ATSimplePermissions serves as a critical security gatekeeper for ALTER TABLE operations. It performs three essential validation checks: (1) verifies that the relation type matches the allowed target types for the specific ALTER operation, (2) confirms that the current user owns the relation, and (3) prevents unauthorized modifications to system catalogs. The function maps relation kinds to internal target type flags and validates them against the operation's allowed targets. If any validation fails, it generates appropriate error messages using the alter_table_type_to_string function for user-friendly reporting.

## Parameters / Member Variables
- : The AlterTableType enumeration specifying the type of ALTER TABLE operation being attempted
- : The Relation structure representing the target relation for the ALTER operation
- : A bitmask of ATT_* flags indicating which relation types are valid targets for this operation

## Dependencies
- Functions called/Symbols referenced:
  - [alter_table_type_to_string](../a/alter_table_type_to_string.md)
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [get_relkind_objtype](../g/get_relkind_objtype.md)
  - [IsSystemRelation](../I/IsSystemRelation.md)
  - RelationGetRelationName
  - RelationGetRelid
  - [GetUserId](../G/GetUserId.md)
- Called from (representative examples):
  - [ATPrepCmd](ATPrepCmd.md) (extensively throughout ALTER TABLE command preparation)
  - [ATExecAddColumn](ATExecAddColumn.md)
  - [ATExecDropColumn](ATExecDropColumn.md)
  - [ATAddCheckConstraint](ATAddCheckConstraint.md)
  - [ATExecDropConstraint](ATExecDropConstraint.md)
  - [ATExecAddInherit](ATExecAddInherit.md)
  - [ATExecAttachPartition](ATExecAttachPartition.md)

## Notes and Other Information
- Maps relation kinds (RELKIND_*) to internal target types (ATT_*) for validation
- Provides comprehensive error reporting with specific messages for wrong object types and permission denials
- Respects the allowSystemTableMods configuration setting for system catalog modifications
- Used extensively throughout the ALTER TABLE command processing pipeline as a standard permission check
- The allowed_targets parameter enables fine-grained control over which relation types can be targeted by specific ALTER operations