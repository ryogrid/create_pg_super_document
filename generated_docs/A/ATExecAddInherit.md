# ATExecAddInherit

## Location
src/backend/commands/tablecmds.c: 15661 - 15772

## Overview
Executes ALTER TABLE INHERIT commands by establishing a new inheritance relationship between child and parent tables after comprehensive validation checks.

## Definition


## Detailed Description
The  function implements the core logic for ALTER TABLE INHERIT operations. It performs extensive validation to ensure the inheritance relationship is valid and safe, then establishes the inheritance link between the child and parent tables. The function handles multiple edge cases including temporary table restrictions, partitioning conflicts, circular inheritance prevention, and trigger compatibility checks.

The function enforces PostgreSQL's inheritance rules by validating ownership permissions, checking table persistence compatibility, preventing inheritance cycles, and ensuring trigger compatibility. Once all validations pass, it delegates to CreateInheritance to establish the actual inheritance relationship.

## Parameters / Member Variables
- : The relation that will inherit from the parent
- : RangeVar specifying the parent relation to inherit from  
- : The lock mode to use during the operation

## Dependencies
- Functions called/Symbols referenced:
  - table_openrv
  - [ATSimplePermissions](ATSimplePermissions.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [list_member_oid](../l/list_member_oid.md)
  - [FindTriggerIncompatibleWithInheritance](../F/FindTriggerIncompatibleWithInheritance.md)
  - [CreateInheritance](../C/CreateInheritance.md)
  - ObjectAddressSet
  - table_close
  - RelationGetRelid
  - RelationGetRelationName
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (for ALTER TABLE INHERIT operations)

## Notes and Other Information
- Requires ShareUpdateExclusiveLock on parent relation to prevent concurrent schema changes
- Enforces multiple inheritance restrictions:
  - Permanent tables cannot inherit from temporary tables
  - Temporary tables must belong to the current session
  - Partitioned tables and partitions cannot participate in inheritance
  - Prevents circular inheritance relationships
- Blocks inheritance when child has ROW triggers with transition tables
- Uses find_all_inheritors to detect potential inheritance cycles
- Returns ObjectAddress of the parent relation for event trigger integration
- Maintains lock on parent relation until transaction commit
- Part of the comprehensive ALTER TABLE infrastructure supporting table inheritance