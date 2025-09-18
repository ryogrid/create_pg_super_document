# change_owner_recurse_to_sequences

## Location
src/backend/commands/tablecmds.c: 14782 - 14850

## Overview
This is a helper function for ATExecChangeOwner that examines the pg_depend system catalog to find sequences dependent on serial columns and recursively changes their ownership to match the table's new owner.

## Definition
```c
static void change_owner_recurse_to_sequences(Oid relationOid, Oid newOwnerId, LOCKMODE lockmode)
```

## Detailed Description
The function implements ownership cascading for sequences that are automatically created by SERIAL or BIGSERIAL column declarations. When a table's ownership changes, any sequences that have auto dependencies on the table's columns must also have their ownership updated to maintain consistency. The function scans the pg_depend catalog looking for sequences with auto or internal dependencies on any column of the specified relation, then recursively calls ATExecChangeOwner to update each sequence's ownership.

The function specifically targets SERIAL sequences by looking for dependencies where:
- The dependency is on a column (refobjsubid > 0)
- The dependent object is a relation (classid = RelationRelationId)
- The dependency type is either DEPENDENCY_AUTO or DEPENDENCY_INTERNAL
- The dependent relation is actually a sequence (relkind = RELKIND_SEQUENCE)

## Parameters / Member Variables
- `relationOid`: The OID of the table whose ownership is being changed and whose dependent sequences need ownership updates
- `newOwnerId`: The OID of the new owner to assign to the dependent sequences
- `lockmode`: The lock mode to use when opening dependent sequence relations

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - ScanKeyInit
  - systable_beginscan
  - systable_getnext
  - systable_endscan
  - relation_open
  - relation_close
  - RelationGetForm
  - ATExecChangeOwner
- Called from (representative examples):
  - ATExecChangeOwner

## Notes and Other Information
- This is a static helper function only accessible within tablecmds.c
- The function maintains proper locking by acquiring AccessShareLock on pg_depend and the specified lockmode on sequence relations
- Only sequences with auto or internal dependencies are processed, ensuring that manually created dependencies are not affected
- The recursive call to ATExecChangeOwner handles the actual ownership change for each sequence
- Located in src/backend/commands/tablecmds.c:14782-14850