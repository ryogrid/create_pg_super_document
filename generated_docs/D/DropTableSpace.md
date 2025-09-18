# DropTableSpace

## Location
[src/backend/commands/tablespace.c:395-571](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablespace.c#L395-L571)

## Overview
Removes a tablespace by validating ownership and emptiness, deleting catalog entries, removing filesystem directories, and logging the operation in WAL with comprehensive dependency checking.

## Definition


## Detailed Description
DropTableSpace implements the DROP TABLESPACE SQL command, performing a complete and safe removal of a tablespace. The function enforces strict preconditions including ownership verification, dependency checking, and emptiness validation before proceeding with removal.

The removal process involves multiple phases: catalog lookup and validation, ownership and dependency checks, metadata cleanup (comments, security labels, dependencies), filesystem removal with retry logic for persistent files, WAL logging, and forced synchronous commit. Special handling addresses platform-specific file deletion issues, particularly Windows file handle persistence.

The function uses TablespaceCreateLock to coordinate with concurrent tablespace operations and implements a sophisticated retry mechanism that triggers checkpoints and process barriers to handle files scheduled for deletion.

## Parameters / Member Variables
- : DropTableSpaceStmt structure containing tablespace name and missing_ok flag for IF EXISTS behavior

## Dependencies
- Functions called/Symbols referenced:
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md): Initiates catalog scan for tablespace lookup
  - [heap_getnext](../h/heap_getnext.md): Retrieves tuples from catalog scan
  - [object_ownercheck](../o/object_ownercheck.md): Verifies current user owns the tablespace
  - [aclcheck_error](../a/aclcheck_error.md): Reports access control violations
  - [IsPinnedObject](../I/IsPinnedObject.md): Checks if tablespace is a system tablespace
  - [checkSharedDependencies](../c/checkSharedDependencies.md): Validates no dependent objects exist
  - InvokeObjectDropHook: Triggers object drop hooks
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md): Removes tuple from system catalog
  - [DeleteSharedComments](DeleteSharedComments.md): Removes associated comments
  - [DeleteSharedSecurityLabel](DeleteSharedSecurityLabel.md): Removes security labels
  - [deleteSharedDependencyRecordsFor](../d/deleteSharedDependencyRecordsFor.md): Cleans up dependency records
  - [destroy_tablespace_directories](../d/destroy_tablespace_directories.md): Removes filesystem structures
  - [RequestCheckpoint](../R/RequestCheckpoint.md): Forces checkpoint to clean lingering files
  - [EmitProcSignalBarrier](../E/EmitProcSignalBarrier.md), WaitForProcSignalBarrier: Coordinates file closure across processes
  - [XLogBeginInsert](../X/XLogBeginInsert.md), XLogRegisterData, XLogInsert: WAL logging functions
  - [ForceSyncCommit](../F/ForceSyncCommit.md): Forces synchronous transaction commit
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md): During SQL command processing

## Notes and Other Information
- Requires ownership of the tablespace (superuser can override via ownership)
- Prohibits dropping of pinned system tablespaces
- Implements comprehensive dependency checking to prevent orphaned objects
- Uses sophisticated retry logic with checkpoint and process barriers for stubborn files
- Addresses Windows-specific file handle persistence issues
- Forces synchronous commit to ensure atomicity between filesystem and catalog changes
- Maintains TablespaceCreateLock during critical filesystem operations
- Supports IF EXISTS syntax through missing_ok parameter