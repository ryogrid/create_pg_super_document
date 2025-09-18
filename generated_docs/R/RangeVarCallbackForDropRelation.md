# RangeVarCallbackForDropRelation

## Location
[src/backend/commands/tablecmds.c:1632-1790](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L1632-L1790)

## Overview
RangeVarCallbackForDropRelation is a callback function that performs permission checks and acquires necessary locks before dropping a relation, ensuring proper authorization and preventing deadlocks through careful lock ordering.

## Definition


## Detailed Description
This function serves as a callback during the relation lookup process for DROP operations. It performs critical safety checks and lock acquisition to ensure the drop operation can proceed safely:

1. **Permission Verification**: Validates that the user has sufficient privileges to drop the relation (either as table owner or schema owner)
2. **Type Validation**: Ensures the relation type matches what is expected for the DROP command
3. **System Catalog Protection**: Prevents dropping system catalogs unless explicitly allowed
4. **Lock Management**: Implements proper lock ordering to prevent deadlocks:
   - For indexes: locks the parent table before the index
   - For partitions: locks the parent partition before the child partition
5. **Invalid Index Handling**: Special handling for invalid system indexes that may need to be dropped after failed concurrent operations

The function also manages cleanup of previously held locks when the relation OID changes between lookups, ensuring no unnecessary locks are maintained.

## Parameters / Member Variables
- : RangeVar representing the relation name being looked up
- : Object ID of the found relation (InvalidOid if not found)
- : Previous relation OID from earlier lookup attempts
- : Pointer to DropRelationCallbackState structure containing callback state

## Dependencies
- Functions called/Symbols referenced:
  - [UnlockRelationOid](../U/UnlockRelationOid.md)
  - [DropErrorMsgWrongType](../D/DropErrorMsgWrongType.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [IsSystemClass](../I/IsSystemClass.md)
  - [IndexGetRelation](../I/IndexGetRelation.md)
  - [LockRelationOid](../L/LockRelationOid.md)
  - [get_partition_parent](../g/get_partition_parent.md)
- Called from (representative examples):
  - [RemoveRelations](RemoveRelations.md)

## Notes and Other Information
- This callback is specifically designed for DROP operations and implements PostgreSQL's lock ordering rules to prevent deadlocks
- The function handles special cases for partitioned tables/indexes and invalid system indexes
- Permission checks allow either table ownership or schema ownership for DROP operations
- The lock management ensures compatibility with regular query patterns where tables are locked before their indexes and parents before partitions
- System catalog protection can be bypassed with allowSystemTableMods setting for maintenance operations