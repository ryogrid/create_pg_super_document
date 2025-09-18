# lo_import_with_oid

## Location
src/interfaces/libpq/fe-lobj.c: 641 - 646

## Overview
Imports a file from the client filesystem into a PostgreSQL large object using a specified OID.

## Definition


## Detailed Description
The  function imports a file from the client's filesystem into a PostgreSQL large object, allowing the caller to specify the desired OID for the large object. This function provides more control over large object creation compared to , which automatically assigns a new OID. It is implemented as a wrapper around , passing the specified OID parameter.

The internal implementation attempts to create a large object with the requested OID using . If the specified OID is already in use, the function will fail and return . Once the large object is successfully created, the function copies the entire contents of the specified file into it using the same chunked reading approach as .

## Parameters / Member Variables
- : PostgreSQL database connection handle  
- : Path to the file on the client filesystem to import
- : Desired OID for the new large object

## Dependencies
- Functions called/Symbols referenced:
  - lo_import_internal
- Internal implementation uses:
  - lo_create
  - lo_open
  - lo_write
  - lo_close
- Called from (representative examples):
  - Client applications requiring specific OID assignment
  - Large object migration or restoration tools

## Notes and Other Information
- Returns the specified OID on success,  on failure
- Will fail if the specified OID is already in use by another large object
- The file is read from the client filesystem, not the server filesystem
- Creates large objects with both read and write permissions
- Handles binary files correctly using binary file opening modes
- For automatic OID assignment, use  instead
- The operation is transactional - if it fails partway through, the transaction is aborted
- Useful for large object backup/restore operations where OID preservation is required