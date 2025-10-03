# ATExecAddInherit

## Location
[src/backend/commands/tablecmds.c:15661-15772](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L15661-L15772)

## Overview
Executes ALTER TABLE INHERIT commands by establishing a new inheritance relationship between child and parent tables after comprehensive validation checks.

## Definition

```c
static ObjectAddress
ATExecAddInherit(Relation child_rel, RangeVar *parent, LOCKMODE lockmode)
```
## Detailed Description
The  function implements the core logic for ALTER TABLE INHERIT operations. It performs extensive validation to ensure the inheritance relationship is valid and safe, then establishes the inheritance link between the child and parent tables. The function handles multiple edge cases including temporary table restrictions, partitioning conflicts, circular inheritance prevention, and trigger compatibility checks.

The function enforces PostgreSQL's inheritance rules by validating ownership permissions, checking table persistence compatibility, preventing inheritance cycles, and ensuring trigger compatibility. Once all validations pass, it delegates to CreateInheritance to establish the actual inheritance relationship.

## Parameters / Member Variables
- `child_rel`: The relation that will inherit from the parent
- `*parent`: RangeVar specifying the parent relation to inherit from
- `lockmode`: The lock mode to use during the operation
## Dependencies
- Functions called/Symbols referenced:
  - [table_openrv](../t/table_openrv.md)
  - [ATSimplePermissions](ATSimplePermissions.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [list_member_oid](../l/list_member_oid.md)
  - [FindTriggerIncompatibleWithInheritance](../F/FindTriggerIncompatibleWithInheritance.md)
  - [CreateInheritance](../C/CreateInheritance.md)
  - ObjectAddressSet
  - [table_close](../t/table_close.md)
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

## Simplified Source

```c
static ObjectAddress
ATExecAddInherit(Relation child_rel, RangeVar *parent, LOCKMODE lockmode)
{
    Relation parent_rel;
    List *children;
    ObjectAddress address;
    const char *trigger_name;

    // Open parent with exclusive lock to prevent concurrent changes
    parent_rel = table_openrv(parent, ShareUpdateExclusiveLock);

    // Check permissions on parent table
    ATSimplePermissions(AT_AddInherit, parent_rel, ATT_TABLE | ATT_FOREIGN_TABLE);

    // Validate table persistence rules
    if (parent_rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP &&
        child_rel->rd_rel->relpersistence != RELPERSISTENCE_TEMP)
        ereport(ERROR, "cannot inherit from temporary relation");

    // Check temp table session ownership
    if (parent_rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP &&
        !parent_rel->rd_islocaltemp)
        ereport(ERROR, "cannot inherit from temporary relation of another session");

    if (child_rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP &&
        !child_rel->rd_islocaltemp)
        ereport(ERROR, "cannot inherit to temporary relation of another session");

    // Reject partitioned tables and partitions
    if (parent_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
        ereport(ERROR, "cannot inherit from partitioned table");

    if (parent_rel->rd_rel->relispartition)
        ereport(ERROR, "cannot inherit from a partition");

    // Prevent circular inheritance by checking if parent inherits from child
    children = find_all_inheritors(RelationGetRelid(child_rel), AccessShareLock, NULL);
    if (list_member_oid(children, RelationGetRelid(parent_rel)))
        ereport(ERROR, "circular inheritance not allowed");

    // Check for incompatible triggers with transition tables
    trigger_name = FindTriggerIncompatibleWithInheritance(child_rel->trigdesc);
    if (trigger_name != NULL)
        ereport(ERROR, "trigger \"%s\" prevents table from becoming inheritance child",
                trigger_name);

    // All validations passed - create the inheritance relationship
    CreateInheritance(child_rel, parent_rel, false);

    // Return parent relation address for event triggers
    ObjectAddressSet(address, RelationRelationId, RelationGetRelid(parent_rel));

    // Keep lock on parent until commit
    table_close(parent_rel, NoLock);
    return address;
}
```