# ATAddForeignKeyConstraint

## Location
[src/backend/commands/tablecmds.c:9607-10047](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L9607-L10047)

## Overview
ATAddForeignKeyConstraint implements the complex logic for adding foreign key constraints to tables, including comprehensive validation, operator resolution, and handling of partitioned table hierarchies.

## Definition

```c
static ObjectAddress
ATAddForeignKeyConstraint(List **wqueue, AlteredTableInfo *tab, Relation rel,
						  Constraint *fkconstraint,
						  bool recurse, bool recursing, LOCKMODE lockmode)
```
## Detailed Description
This function is one of PostgreSQL's most complex constraint implementation functions, handling the complete lifecycle of foreign key constraint creation. It performs extensive validation of referencing and referenced tables, resolves appropriate equality operators for each column pair, handles table persistence compatibility checks, and manages the intricate requirements of partitioned table foreign keys.

The function implements sophisticated operator resolution logic to find appropriate equality operators for comparing foreign key columns with primary key columns, including support for implicit type coercion and polymorphic types. It validates that the referenced columns form a unique constraint and checks permissions on both sides of the relationship.

For partitioned tables, the function coordinates the creation of multiple pg_constraint entries and associated triggers across all partitions. It includes optimization logic to avoid revalidating existing data when constraints are modified in compatible ways, and handles the complex case of generated columns with appropriate action restrictions.

The implementation follows a three-phase approach: first creating the catalog entry, then processing action triggers on the referenced side, and finally creating check triggers on the referencing side, with appropriate recursion handling for inheritance hierarchies.

## Parameters / Member Variables
- : Double pointer to the work queue for coordinating ALTER TABLE operations across multiple tables
- : AlteredTableInfo structure containing information about the table being altered
- : Relation object representing the referencing (foreign key) table
- : Constraint specification containing all foreign key definition details
- : Boolean indicating whether to apply the constraint to inheritance children
- : Boolean indicating if this is a recursive call (affects permission handling)
- : Lock mode to use when accessing related tables during the operation

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md), table_openrv
  - [transformColumnNameList](../t/transformColumnNameList.md)
  - [transformFkeyGetPrimaryKey](../t/transformFkeyGetPrimaryKey.md)
  - [transformFkeyCheckAttrs](../t/transformFkeyCheckAttrs.md)
  - [checkFkeyPermissions](../c/checkFkeyPermissions.md)
  - [validateFkOnDeleteSetColumns](../v/validateFkOnDeleteSetColumns.md)
  - [get_opfamily_member](../g/get_opfamily_member.md)
  - [can_coerce_type](../c/can_coerce_type.md)
  - [findFkeyCast](../f/findFkeyCast.md)
  - [addFkConstraint](../a/addFkConstraint.md)
  - [addFkRecurseReferenced](../a/addFkRecurseReferenced.md)
  - [addFkRecurseReferencing](../a/addFkRecurseReferencing.md)
- Called from (representative examples):
  - [ATExecAddConstraint](ATExecAddConstraint.md)

## Notes and Other Information
- Handles complex operator resolution for type compatibility between foreign and primary key columns
- Implements comprehensive validation including table persistence compatibility (permanent/unlogged/temporary)
- Supports optimization to avoid revalidation when constraints are modified in compatible ways
- Manages the intricate requirements of partitioned table foreign keys with multiple constraint entries
- Enforces restrictions on generated columns according to SQL standard requirements
- Coordinates trigger creation on both referencing and referenced sides of the relationship
- Includes sophisticated error handling with detailed diagnostic messages for incompatible types
- Integrates with PostgreSQL's work queue system for managing complex multi-table operations
- Essential component of PostgreSQL's referential integrity implementation
- One of the most complex functions in the ALTER TABLE subsystem due to the inherent complexity of foreign key semantics