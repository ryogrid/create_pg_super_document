# exportFile

## Location
src/test/examples/testlo64.c: 172 - 214

## Overview
A static function that exports a PostgreSQL large object to a file on the Unix filesystem by copying its contents.

## Definition


## Detailed Description
The  function provides functionality to export a PostgreSQL large object to an external Unix file. It opens the specified large object in read-only mode, creates or truncates the target file, and copies the large object's contents to the file using a buffered read/write approach. The function handles the complete export process including proper file creation, data transfer in chunks, and cleanup of both database and file descriptors.

## Parameters / Member Variables
- : Database connection handle for PostgreSQL operations
- : OID of the large object to export from the database
- : Path to the Unix file where the large object contents will be written

## Dependencies
- Functions called/Symbols referenced:
  - lo_open (PostgreSQL large object opening)
  - lo_read (PostgreSQL large object reading)
  - lo_close (PostgreSQL large object closing)
  - open (Unix system call for file operations)
  - write (Unix system call for writing file data)
  - close (Unix system call for closing file)
  - fprintf (formatted output to stderr)
  - BUFSIZE (buffer size constant)
  - INV_READ (large object access mode constant)
  - O_CREAT, O_WRONLY, O_TRUNC (file creation and access mode constants)
- Called from (representative examples):
  - No direct callers found (static function in test example)

## Notes and Other Information
- This is a static function located in the test examples directory, primarily used for demonstration purposes
- Creates the target file with permissions 0666 (read/write for owner, group, and others)
- Uses file creation flags O_CREAT | O_WRONLY | O_TRUNC to create/truncate the output file
- Uses a buffer size defined by BUFSIZE constant for efficient data copying
- Opens large objects with read-only permission (INV_READ) for safety
- Error handling includes basic fprintf statements for debugging file and write operations
- Performs the inverse operation of importFile - extracting data from large objects to external files
- Part of the testlo.c example demonstrating bidirectional large object file operations in PostgreSQL
- Properly handles partial writes by checking if the number of bytes written matches what was read