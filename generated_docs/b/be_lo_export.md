# be_lo_export

## Location
src/backend/libpq/be-fsstubs.c: 481 - 552

## Overview
Backend function that exports a PostgreSQL large object to a server-side file system file, implementing the lo_export() SQL function.

## Definition
Datum be_lo_export(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as the backend implementation for PostgreSQL's lo_export() SQL function. It reads data from an existing large object and writes it to a file on the server's file system. The function is designed to be called from SQL and follows PostgreSQL's function calling conventions using the PG_FUNCTION_ARGS interface.

Key operational aspects:
1. Opens the specified large object in read-only mode
2. Creates/truncates the target file with appropriate permissions (644: owner read/write, group/others read-only)
3. Temporarily modifies the umask to ensure consistent file permissions (022 instead of default 077)
4. Reads data from the large object in chunks and writes to the file
5. Handles all file and large object operations with proper error reporting
6. Uses PG_TRY/PG_FINALLY blocks to ensure umask is restored even if errors occur

## Parameters / Member Variables
- : OID of the large object to export
- : Text containing the target file path on the server

## Dependencies
- Functions called/Symbols referenced:
  - [inv_open](../i/inv_open.md)
  - text_to_cstring_buffer
  - umask
  - OpenTransientFilePerm
  - [inv_read](../i/inv_read.md)
  - write
  - CloseTransientFile
  - [inv_close](../i/inv_close.md)
- Called from (representative examples):
  - SQL function lo_export() (via function manager)

## Notes and Other Information
- This is a PostgreSQL internal function registered in the system catalog
- Requires superuser privileges to execute (server-side file access)
- Uses BUFSIZE (8192 bytes) for efficient buffered I/O operations
- Sets lo_cleanup_needed flag for proper cleanup handling
- File permissions are set to 644 (owner read/write, others read-only) for security
- Returns 1 on success as per PostgreSQL convention
- Will overwrite existing files (uses O_TRUNC flag)
- Proper umask handling ensures consistent file permissions across different system configurations