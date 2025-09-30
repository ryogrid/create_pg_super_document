# renameatt

## Location
[src/backend/commands/tablecmds.c:3877-3914](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L3877-L3914)

## Overview
renameatt is the top-level function that handles the renaming of an attribute (column) in a PostgreSQL relation, serving as the main entry point for ALTER TABLE RENAME COLUMN commands.

## Definition
ObjectAddress renameatt(RenameStmt *stmt)

## Detailed Description
renameatt processes a RENAME COLUMN statement by first acquiring an exclusive lock on the target relation, then delegating the actual renaming work to renameatt_internal. The function handles missing relations gracefully when the missing_ok flag is set, issuing a notice instead of an error. It performs the necessary permission checks through the RangeVarCallbackForRenameAttribute callback before proceeding with the rename operation.

The function returns an ObjectAddress that identifies the renamed column, making it suitable for dependency tracking and event triggers. The lock level used (AccessExclusiveLock) matches that used by renameatt_internal to ensure consistency across the operation.

## Parameters / Member Variables
- stmt: RenameStmt structure containing the rename operation details
  - relation: The target relation to modify
  - subname: Current name of the attribute to rename
  - newname: New name for the attribute
  - missing_ok: Whether to silently skip if relation does not exist
  - behavior: Drop behavior (CASCADE or RESTRICT)

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md)
  - AccessExclusiveLock
  - RVR_MISSING_OK
  - [RangeVarCallbackForRenameAttribute](../R/RangeVarCallbackForRenameAttribute.md)
  - [renameatt_internal](renameatt_internal.md)
  - ObjectAddressSubSet
  - ereport/NOTICE
- Called from (representative examples):
  - [ExecRenameStmt](../E/ExecRenameStmt.md) (in src/backend/commands/alter.c)

## Notes and Other Information
- Uses AccessExclusiveLock to prevent concurrent modifications during the rename operation
- Handles inheritance hierarchies through the inh flag passed to renameatt_internal
- Returns InvalidObjectAddress if the relation does not exist and missing_ok is true
- The function is designed to be called from the SQL command execution path
- Part of the broader table command infrastructure in PostgreSQL

## Simplified Source

```c
ObjectAddress
renameatt(RenameStmt *stmt)
{
    Oid relid;
    AttrNumber attnum;
    ObjectAddress address;

    // Get relation OID with exclusive lock and permission checks
    relid = RangeVarGetRelidExtended(stmt->relation, AccessExclusiveLock,
                                     stmt->missing_ok ? RVR_MISSING_OK : 0,
                                     RangeVarCallbackForRenameAttribute,
                                     NULL);

    // Handle missing relation gracefully if requested
    if (!OidIsValid(relid)) {
        ereport(NOTICE, "relation does not exist, skipping");
        return InvalidObjectAddress;
    }

    // Delegate to internal implementation
    attnum = renameatt_internal(relid,
                               stmt->subname,    /* old attribute name */
                               stmt->newname,    /* new attribute name */
                               stmt->relation->inh,  /* recursive? */
                               false,           /* recursing? */
                               0,               /* expected inhcount */
                               stmt->behavior); /* drop behavior */

    // Return object address for the renamed column
    ObjectAddressSubSet(address, RelationRelationId, relid, attnum);

    return address;
}
```