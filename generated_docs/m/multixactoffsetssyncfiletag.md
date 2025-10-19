# multixactoffsetssyncfiletag

## Location
[src/backend/access/transam/multixact.c:3567-3575](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L3567-L3575)

## Overview
multixactoffsetssyncfiletag is an entrypoint function for sync.c to synchronize multixact offsets files to disk using the SLRU synchronization infrastructure.

## Definition

```c
int
multixactoffsetssyncfiletag(const FileTag *ftag, char *path)
```
## Detailed Description
This function serves as a wrapper around SlruSyncFileTag specifically for multixact offsets files. It is called by the sync.c subsystem when PostgreSQL needs to ensure that multixact offset SLRU pages are properly synchronized to disk. The function delegates the actual synchronization work to the generic SLRU sync mechanism while providing the correct control structure (MultiXactOffsetCtl) for multixact offsets.

## Parameters / Member Variables
- `*ftag`: Pointer to a FileTag structure identifying the specific file to be synchronized
- `*path`: Character pointer to the file path for the file being synchronized
## Dependencies
- Functions called/Symbols referenced:
  - [SlruSyncFileTag](../S/SlruSyncFileTag.md)
  - MultiXactOffsetCtl
  - [FileTag](../F/FileTag.md) (type)
- Called from:
  - Referenced by SizeOfMultiXactTruncate in src/include/access/multixact.h

## Notes and Other Information
- Part of the file synchronization infrastructure used by PostgreSQL's sync.c
- Specifically handles multixact offset files within the SLRU (Simple LRU) system
- Returns an integer result from the underlying SlruSyncFileTag function
- Provides a type-safe interface for multixact offset file synchronization
- Located in src/backend/access/transam/multixact.c:3567-3575

## Simplified Source

```c
int multixactoffsetssyncfiletag(const FileTag *ftag, char *path)
{
    // Delegate to generic SLRU file sync using MultiXact offset control structure
    return SlruSyncFileTag(MultiXactOffsetCtl, ftag, path);
}
```