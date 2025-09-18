# lo_import_internal

## Location
[src/backend/libpq/be-fsstubs.c:419-480](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-fsstubs.c#L419-L480)

## Overview
Internal function that imports data from a server-side file into a large object, creating a new large object or using a specified OID.

## Definition
static Oid lo_import_internal(text *filename, Oid lobjOid)

## Detailed Description
This static function handles the core logic for importing file data into PostgreSQL's large object storage system. It reads data from a server-side file specified by filename and writes it to a newly created large object. The function operates entirely on the server side, requiring appropriate file system permissions to access the source file.

The function performs several key operations:
1. Validates that the operation is allowed (prevents execution in read-only transactions)
2. Opens the specified file for reading in binary mode
3. Creates a new large object (either with a specified OID or auto-generated)
4. Reads data from the file in chunks and writes it to the large object
5. Properly closes both the file and large object handles

## Parameters / Member Variables
- : Text parameter containing the path to the server-side file to import
- : OID to use for the new large object (if InvalidOid, a new OID will be generated)

## Dependencies
- Functions called/Symbols referenced:
  - [PreventCommandIfReadOnly](../P/PreventCommandIfReadOnly.md)
  - text_to_cstring_buffer
  - OpenTransientFile
  - [inv_create](../i/inv_create.md)
  - [inv_open](../i/inv_open.md)
  - [inv_write](../i/inv_write.md)
  - [inv_close](../i/inv_close.md)
  - CloseTransientFile
- Called from (representative examples):
  - [be_lo_import](../b/be_lo_import.md)
  - [be_lo_import_with_oid](../b/be_lo_import_with_oid.md)

## Notes and Other Information
- This is a static function, only accessible within be-fsstubs.c
- Uses BUFSIZE (8192 bytes) for buffered file reading
- Sets lo_cleanup_needed flag to ensure proper cleanup in case of errors
- Performs comprehensive error handling for file operations
- Only works with server-side files, not client-side files
- Requires appropriate file system permissions on the PostgreSQL server