# makeBufFileCommon

## Location
[src/backend/storage/file/buffile.c:118-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L118-L138)

## Overview
Creates and initializes a BufFile structure with common default settings for buffered file operations in PostgreSQL.

## Definition
```c
static BufFile *makeBufFileCommon(int nfiles)
```

## Detailed Description
makeBufFileCommon is an internal helper function that creates a new BufFile structure and performs common initialization tasks. It allocates memory for the BufFile structure and sets up default values for all its fields. This function serves as the foundation for various BufFile creation functions like makeBufFile and BufFileCreateFileSet, providing consistent initialization across different BufFile creation scenarios.

The function initializes the BufFile with safe defaults: not inter-transaction persistent, not dirty, associated with the current resource owner, and positioned at the beginning of the first file with no buffered data.

## Parameters / Member Variables
- `nfiles`: The number of files that will be associated with this BufFile (determines the size of the files array)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - CurrentResourceOwner (global variable for resource management)
- Called from (representative examples):
  - [makeBufFile](makeBufFile.md)
  - [BufFileCreateFileSet](../B/BufFileCreateFileSet.md)
  - [BufFileOpenFileSet](../B/BufFileOpenFileSet.md)

## Notes and Other Information
- This is a static function internal to buffile.c, not exposed to external modules
- The function sets isInterXact to false by default, meaning the BufFile will be cleaned up at transaction end
- All position tracking fields (curFile, curOffset, pos, nbytes) are initialized to 0
- The dirty flag is initialized to false, indicating no pending writes
- The function relies on palloc for memory allocation, which will automatically handle cleanup on transaction abort