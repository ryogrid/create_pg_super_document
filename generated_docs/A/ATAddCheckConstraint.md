# ATAddCheckConstraint

## Location
[src/backend/commands/tablecmds.c:9470-9606](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L9470-L9606)

## Overview
ATAddCheckConstraint adds a check constraint to a table and recursively applies it to all child tables in an inheritance hierarchy, ensuring consistent constraint naming across the hierarchy.

## Definition

```c
static ObjectAddress
ATAddCheckConstraint(List **wqueue, AlteredTableInfo *tab, Relation rel,
					 Constraint *constr, bool recurse, bool recursing,
					 bool is_readd, LOCKMODE lockmode)
```
## Detailed Description
This function implements check constraint addition with sophisticated inheritance handling. Unlike other ALTER TABLE operations that use prep-time recursion, this function performs execution-time recursion to ensure all constraints across the inheritance hierarchy receive the same name. This is critical because PostgreSQL requires related constraints to have identical names to be recognized as part of the same logical constraint.

The function uses AddRelationNewConstraints to create the actual constraint, handling constraint merging when appropriate (particularly for child tables that may already have compatible constraints). It manages a work queue system for deferred validation and carefully tracks whether constraints need validation through the NewConstraint structure.

For inheritance hierarchies, the function recursively descends one level at a time rather than using find_all_inheritors, allowing precise control over constraint propagation and name consistency. It includes safety checks for ONLY clauses and NO INHERIT constraints.

## Parameters / Member Variables
- `**wqueue`: Double pointer to the work queue for managing ALTER TABLE operations across multiple tables
- `*tab`: AlteredTableInfo structure for the current table being modified
- `rel`: Relation object representing the table receiving the constraint
- `*constr`: Constraint specification including the check expression and properties
- `recurse`: Boolean indicating whether to apply the constraint to child tables
- `recursing`: Boolean indicating if this is a recursive call (affects permission checking)
- `is_readd`: Boolean indicating if this constraint is being re-added during a table rewrite
- `lockmode`: Lock mode to use when accessing child tables
## Dependencies
- Functions called/Symbols referenced:
  - [ATSimplePermissions](ATSimplePermissions.md)
  - [AddRelationNewConstraints](AddRelationNewConstraints.md)
  - copyObject
  - [find_inheritance_children](../f/find_inheritance_children.md)
  - [CheckAlterTableIsSafe](../C/CheckAlterTableIsSafe.md)
  - [ATGetQueueEntry](ATGetQueueEntry.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - ObjectAddressSet
- Called from (representative examples):
  - [ATExecAddConstraint](ATExecAddConstraint.md)
  - [DetachAddConstraintIfNeeded](../D/DetachAddConstraintIfNeeded.md)
  - [ATAddCheckConstraint](ATAddCheckConstraint.md) (recursive calls)

## Notes and Other Information
- Performs execution-time rather than prep-time recursion to ensure consistent constraint naming
- Handles constraint merging for cases where child tables already have compatible constraints
- Includes sophisticated inheritance handling with proper lock management
- Supports NO INHERIT constraints that don't propagate to children
- Validates ONLY clause usage and prevents constraint addition when children exist but recursion is disabled
- Integrates with the work queue system for deferred constraint validation
- Uses CommandCounterIncrement to handle multiple visits to the same table
- Critical for maintaining constraint consistency across PostgreSQL inheritance hierarchies

## Simplified Source

```c
static ObjectAddress ATAddCheckConstraint(List **wqueue, AlteredTableInfo *tab, Relation rel,
                                         Constraint *constr, bool recurse, bool recursing,
                                         bool is_readd, LOCKMODE lockmode) {
    List *newcons;
    ListCell *lcon, *child;
    List *children;
    ObjectAddress address = InvalidObjectAddress;

    // Check permissions if this is a recursive call
    if (recursing)
        ATSimplePermissions(AT_AddConstraint, rel, ATT_TABLE | ATT_FOREIGN_TABLE);

    // Add constraint to current table using copy to preserve original
    newcons = AddRelationNewConstraints(rel, NIL,
                                       list_make1(copyObject(constr)),
                                       recursing || is_readd,  /* allow_merge */
                                       !recursing,             /* is_local */
                                       is_readd,               /* is_internal */
                                       NULL);

    // Process each new constraint (typically just one)
    foreach(lcon, newcons) {
        CookedConstraint *ccon = (CookedConstraint *) lfirst(lcon);

        // Add to validation queue if needed
        if (!ccon->skip_validation) {
            NewConstraint *newcon = palloc0(sizeof(NewConstraint));
            newcon->name = ccon->name;
            newcon->contype = ccon->contype;
            newcon->qual = ccon->expr;
            tab->constraints = lappend(tab->constraints, newcon);
        }

        // Save assigned constraint name and set return address
        if (constr->conname == NULL)
            constr->conname = ccon->name;
        ObjectAddressSet(address, ConstraintRelationId, ccon->conoid);
    }

    CommandCounterIncrement();

    // If constraint was merged or is NO INHERIT, we're done
    if (newcons == NIL || constr->is_no_inherit)
        return address;

    // Find child tables for inheritance
    children = find_inheritance_children(RelationGetRelid(rel), lockmode);

    // Error if ONLY specified but children exist
    if (!recurse && children != NIL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                       errmsg("constraint must be added to child tables too")));

    // Recursively apply constraint to each child table
    foreach(child, children) {
        Oid childrelid = lfirst_oid(child);
        Relation childrel = table_open(childrelid, NoLock);
        AlteredTableInfo *childtab;

        CheckAlterTableIsSafe(childrel);
        childtab = ATGetQueueEntry(wqueue, childrel);

        // Recurse to child with same constraint name
        ATAddCheckConstraint(wqueue, childtab, childrel, constr,
                            recurse, true, is_readd, lockmode);

        table_close(childrel, NoLock);
    }

    return address;
}
```