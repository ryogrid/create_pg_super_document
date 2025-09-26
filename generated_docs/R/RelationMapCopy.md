# RelationMapCopy

## Location
[src/backend/utils/cache/relmapper.c:292-324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L292-L324)

## Overview
Copies a relation mapping file from a source database path to a destination database path, with WAL logging, for creating new databases.

## Definition
void RelationMapCopy(Oid dbid, Oid tsid, char *srcdbpath, char *dstdbpath)

## Detailed Description
This function facilitates database creation by copying relation mapping information from a source database to a newly created destination database. It reads the complete relation mapping file from the source database directory and writes it to the destination database directory, ensuring the new database has proper relation-to-file mappings.

The operation is WAL-logged to ensure crash recovery consistency and durability. The function is specifically designed for creating new databases that don't yet have relation mapping files, not for replacing existing ones.

The function operates under RelationMappingLock to ensure atomicity and prevent conflicts with concurrent mapping operations. Since the destination database is not yet accessible to users, no shared invalidation messages are required.

## Parameters / Member Variables
- `dbid`: Database OID of the destination database
- `tsid`: Tablespace OID where the destination database is located
- `srcdbpath`: File system path to the source database directory
- `dstdbpath`: File system path to the destination database directory

## Dependencies
- Functions called/Symbols referenced:
  - [RelMapFile](RelMapFile.md) (structure for holding mapping data)
  - [read_relmap_file](../r/read_relmap_file.md) (reads mapping from source location)
  - [write_relmap_file](../w/write_relmap_file.md) (writes mapping to destination with WAL logging)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (RelationMappingLock for concurrency control)
- Called from (representative examples):
  - [CreateDatabaseUsingWalLog](../C/CreateDatabaseUsingWalLog.md) (dbcommands.c:169)

## Notes and Other Information
- Intended only for creating new databases, not for replacing existing relation mapping files
- WAL logs the operation for crash recovery and replication consistency
- No shared invalidation needed since destination database is not yet accessible to clients
- Uses exclusive RelationMappingLock to ensure atomic operation
- The function does not attempt to preserve existing files in the destination since the new database is not yet usable
- Failure during this operation would make the new database unusable, so error handling is critical