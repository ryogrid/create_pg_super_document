# FileTag

## Location
[src/include/storage/sync.h:50-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/sync.h#L50-L56)

## Overview
FileTag is a structure that uniquely identifies a file in PostgreSQL's storage synchronization system, providing all the necessary information to locate and handle files during sync operations.

## Definition

```c
typedef struct FileTag
{
	int16		handler;		/* SyncRequestHandler value, saving space */
	int16		forknum;		/* ForkNumber, saving space */
	RelFileLocator rlocator;
	uint64		segno;
} FileTag;
```
## Detailed Description
FileTag serves as a comprehensive identifier for files in PostgreSQL's storage synchronization framework. It encapsulates all the information needed to uniquely identify a physical file and determine how it should be processed during sync operations. The structure is designed to be compact while providing sufficient detail for the sync system to route requests to appropriate handlers.

The FileTag abstraction allows the sync system (sync.c) to remain agnostic about the internal structure and meaning of the identifiers, while providing enough information for various file handlers (md.c, SLRU modules, etc.) to locate and operate on their respective files. This design enables extensibility as new file types can be added with their own handlers without modifying the core sync logic.

The structure is specifically optimized for use in hash tables and as a key for sync request tracking, which is why space-saving 16-bit integers are used for handler and forknum fields, and the struct must not contain padding bytes.

## Parameters / Member Variables
- `handler`: Specifies which SyncRequestHandler should process this file (SYNC_HANDLER_MD for regular relations, SYNC_HANDLER_CLOG for transaction logs, etc.)
- `forknum`: Identifies the specific fork of a relation (MAIN_FORKNUM for data, FSM_FORKNUM for free space map, VISIBILITYMAP_FORKNUM for visibility map, INIT_FORKNUM for unlogged relations)
- `rlocator`: Contains the RelFileLocator with tablespace OID, database OID, and relation file number to fully locate the relation
- `segno`: Segment number within the file, used when files are split into multiple segments (typically 1GB each)
## Dependencies
- Functions called/Symbols referenced:
  - [RelFileLocator](../R/RelFileLocator.md) (for physical file location)
  - SyncRequestHandler (for routing to appropriate handler)
  - [ForkNumber](ForkNumber.md) (for identifying relation forks)
- Called from (representative examples):
  - [RememberSyncRequest](../R/RememberSyncRequest.md) (registers sync requests using FileTag)
  - [RegisterSyncRequest](../R/RegisterSyncRequest.md) (submits sync requests with FileTag identification)
  - Various md.c functions for managing relation files
  - SLRU modules for transaction log file management

## Notes and Other Information
- [FileTag](FileTag.md) is designed to be used as a hashtable key, requiring no padding bytes between members
- The structure is liable to change as required by future sync handlers, maintaining flexibility
- Space optimization using int16 for handler and forknum allows the structure to remain compact
- Currently tailored for md.c usage but abstracted enough for sync.c to remain handler-agnostic
- The sync system uses FileTag to track pending fsync operations, unlink requests, and other file operations during checkpoint processing
- Different handlers interpret the fields differently: md.c uses all fields, while SLRU handlers may use a subset depending on their specific file organization