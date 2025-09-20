# multixactmemberssyncfiletag

## Location
[src/backend/access/transam/multixact.c:3576-3579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L3576-L3579)

## Overview
multixactmemberssyncfiletag is an entrypoint function for sync.c to synchronize multixact members files to disk using the SLRU synchronization infrastructure.

## Definition

```c
int
multixactmemberssyncfiletag(const FileTag *ftag, char *path)
```
## Detailed Description
This function serves as a wrapper around SlruSyncFileTag specifically for multixact members files. It is called by the sync.c subsystem when PostgreSQL needs to ensure that multixact member SLRU pages are properly synchronized to disk. The function delegates the actual synchronization work to the generic SLRU sync mechanism while providing the correct control structure (MultiXactMemberCtl) for multixact members.

## Parameters / Member Variables
- : Pointer to a FileTag structure identifying the specific file to be synchronized
- : Character pointer to the file path for the file being synchronized

## Dependencies
- Functions called/Symbols referenced:
  - [SlruSyncFileTag](../S/SlruSyncFileTag.md)
  - MultiXactMemberCtl
  - FileTag (type)
- Called from:
  - Referenced by SizeOfMultiXactTruncate in src/include/access/multixact.h

## Notes and Other Information
- Part of the file synchronization infrastructure used by PostgreSQL's sync.c
- Specifically handles multixact member files within the SLRU (Simple LRU) system
- Returns an integer result from the underlying SlruSyncFileTag function
- Provides a type-safe interface for multixact member file synchronization
- Companion function to multixactoffsetssyncfiletag, handling the member data files
- Located in src/backend/access/transam/multixact.c:3576-3579