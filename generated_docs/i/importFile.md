# importFile

## Location
[src/test/examples/testlo64.c:34-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/examples/testlo64.c#L34-L77)

## Overview
A static function that imports a file from the Unix filesystem into the PostgreSQL database as a large object.

## Definition


## Detailed Description
The  function provides functionality to import external files into PostgreSQL as large objects (LOBs). It opens a specified Unix file, creates a new large object in the database, and copies the file's contents into the large object using a buffered read/write approach. The function handles the complete lifecycle of large object creation including opening, writing data in chunks, and proper cleanup of file descriptors.

## Parameters / Member Variables
- : Database connection handle for PostgreSQL operations
- : Path to the Unix file to be imported into the database

## Dependencies
- Functions called/Symbols referenced:
  - open (Unix system call for file operations)
  - [lo_creat](../l/lo_creat.md) (PostgreSQL large object creation)
  - [lo_open](../l/lo_open.md) (PostgreSQL large object opening)
  - read (Unix system call for reading file data)
  - [lo_write](../l/lo_write.md) (PostgreSQL large object writing)
  - close (Unix system call for closing file)
  - [lo_close](../l/lo_close.md) (PostgreSQL large object closing)
  - BUFSIZE (buffer size constant)
  - INV_READ, INV_WRITE (large object access mode constants)
- Called from (representative examples):
  - No direct callers found (static function in test example)

## Notes and Other Information
- This is a static function located in the test examples directory, primarily used for demonstration purposes
- Uses a buffer size defined by BUFSIZE constant for efficient file copying
- Creates large objects with both read and write permissions (INV_READ | INV_WRITE)
- Error handling includes basic fprintf statements for debugging
- Returns the OID of the created large object for further reference
- Part of the testlo.c example demonstrating large object operations in PostgreSQL