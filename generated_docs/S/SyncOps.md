# SyncOps

## Location
[src/backend/storage/sync/sync.c:84-90](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/sync/sync.c#L84-L90)

## Overview
SyncOps is a function pointer structure that defines the interface for handling synchronization and unlink operations on different types of storage files in PostgreSQL's storage subsystem.

## Definition

```c
typedef struct SyncOps
{
	int			(*sync_syncfiletag) (const FileTag *ftag, char *path);
	int			(*sync_unlinkfiletag) (const FileTag *ftag, char *path);
	bool		(*sync_filetagmatches) (const FileTag *ftag,
										const FileTag *candidate);
} SyncOps;
```
## Detailed Description
SyncOps serves as a polymorphic interface for handling file synchronization operations across different storage handlers in PostgreSQL. The structure contains function pointers that allow different storage subsystems (magnetic disk, transaction logs, commit timestamps, multixact data) to implement their own specific synchronization logic while maintaining a uniform interface. This design enables the sync manager to handle various file types without needing to know the specific implementation details of each storage type.

The structure is used in conjunction with the syncsw array, which maps SyncRequestHandler enum values to their corresponding SyncOps implementations, providing a dispatch table for routing sync operations to the appropriate handler.

## Parameters / Member Variables
- `*path)`: Function pointer for synchronizing a file identified by a FileTag to persistent storage, returns int status
- `*path)`: Function pointer for unlinking/removing a file identified by a FileTag, returns int status
- `*candidate)`: Function pointer for determining if two FileTag structures match/refer to the same logical file, returns boolean result
## Dependencies
- Symbols referenced:
  - [FileTag](../F/FileTag.md) (used as parameter type in all function pointers)
- Used by:
  - syncsw array (static array of SyncOps structures indexed by SyncRequestHandler values)
  - Various storage handler functions (mdsyncfiletag, mdunlinkfiletag, mdfiletagmatches, clogsyncfiletag, etc.)

## Notes and Other Information
- The structure is defined in src/backend/storage/sync/sync.c:84-90
- Each function pointer may be NULL for handlers that don't support that particular operation (e.g., some handlers only implement sync_syncfiletag)
- The design follows the strategy pattern, allowing different storage subsystems to provide their own implementations
- Currently used by handlers for: magnetic disk (MD), transaction logs (CLOG), commit timestamps, and multixact data
- Part of PostgreSQL's centralized file synchronization system that ensures data durability across different storage components