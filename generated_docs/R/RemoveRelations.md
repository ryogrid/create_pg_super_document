# RemoveRelations

## Location
[src/backend/commands/tablecmds.c:1468-1631](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L1468-L1631)

## Overview
RemoveRelations implements the core functionality for DROP TABLE, DROP INDEX, DROP SEQUENCE, DROP VIEW, DROP MATERIALIZED VIEW, and DROP FOREIGN TABLE commands.

## Definition


## Detailed Description
RemoveRelations is the main function responsible for handling various DROP statements for database relations. It processes a DropStmt parse tree and coordinates the deletion of one or more relations. The function operates in two phases: first identifying and validating all relations to be dropped, then performing the actual deletions in a single batch operation. It handles special cases like concurrent drops, partitioned indexes, and dependency validation. The function maps different DROP command types to their corresponding relation kinds, performs appropriate locking, validates permissions and constraints, and finally invokes performMultipleDeletions to remove the objects from the system catalogs and file system.

## Parameters / Member Variables
- : DropStmt structure containing the parsed DROP statement with object names, drop behavior, and options

## Dependencies
- Functions called/Symbols referenced:
  - [new_object_addresses](../n/new_object_addresses.md)
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
  - [RangeVarGetRelidExtended](RangeVarGetRelidExtended.md)
  - [RangeVarCallbackForDropRelation](RangeVarCallbackForDropRelation.md)
  - [DropErrorMsgNonExistent](../D/DropErrorMsgNonExistent.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - [performMultipleDeletions](../p/performMultipleDeletions.md)
  - [free_object_addresses](../f/free_object_addresses.md)
- Called from (representative examples):
  - ExecDropStmt

## Notes and Other Information
RemoveRelations supports concurrent index dropping with ShareUpdateExclusiveLock, but restricts it to single objects without CASCADE behavior. The function handles partitioned indexes specially by pre-locking all child table partitions to avoid deadlocks. It validates relation types against expected kinds and provides appropriate error messages through helper functions. The two-phase approach (identify first, then delete) prevents unwanted DROP RESTRICT errors when relations have dependencies among themselves. The function processes shared-cache invalidation messages before relation lookups to handle cases where relations were dropped and recreated during the transaction.