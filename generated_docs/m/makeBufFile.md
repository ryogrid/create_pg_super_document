# makeBufFile

## Location
[src/backend/storage/file/buffile.c:139-155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L139-L155)

## Overview
Creates a BufFile structure for a single underlying physical file, providing buffered I/O operations on top of PostgreSQL File handles.

## Definition
```c
static BufFile *makeBufFile(File firstfile)
```

## Detailed Description
makeBufFile is an internal helper function that creates a BufFile for managing buffered I/O operations on a single physical file. It builds upon makeBufFileCommon to handle the common initialization, then sets up the file-specific components including allocating space for one File handle and initializing file-specific fields.

The function creates a BufFile that is not read-only by default and is not associated with a FileSet or named file. The caller is responsible for setting the isInterXact flag if the BufFile should persist beyond the current transaction.

## Parameters / Member Variables
- `firstfile`: A File handle representing the underlying physical file that this BufFile will manage

## Dependencies
- Functions called/Symbols referenced:
  - [makeBufFileCommon](makeBufFileCommon.md) (common BufFile initialization)
  - [palloc](../p/palloc.md) (memory allocation for the files array)
- Called from (representative examples):
  - [BufFileCreateTemp](../B/BufFileCreateTemp.md)

## Notes and Other Information
- This is a static function internal to buffile.c, not exposed to external modules
- The function allocates space for exactly one File handle in the files array
- Sets readOnly to false, allowing both read and write operations
- The fileset and name fields are set to NULL, indicating this is not a named or FileSet-managed file
- The caller must explicitly set isInterXact if the BufFile should survive transaction boundaries
- This function is typically used for temporary files that exist only within a single transaction

## Simplified Source

```c
static BufFile *
makeBufFile(File firstfile)
{
    // Create common BufFile structure for single file
    BufFile *file = makeBufFileCommon(1);

    // Allocate and set up file array with the provided file handle
    file->files = (File *) palloc(sizeof(File));
    file->files[0] = firstfile;

    // Initialize file properties
    file->readOnly = false;    // Allow read and write operations
    file->fileset = NULL;      // Not part of a FileSet
    file->name = NULL;         // Not a named file

    return file;
}
```