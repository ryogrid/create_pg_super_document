# be_lo_import_with_oid

## Location
src/backend/libpq/be-fsstubs.c: 410 - 418

## Overview
Imports a file from the filesystem into the database as a large object with a specific user-provided OID.

## Definition


## Detailed Description
This function implements the backend functionality for the SQL  function, which allows importing external files into PostgreSQL as large objects while specifying the desired OID for the large object. Unlike  which auto-generates an OID, this function allows the caller to specify exactly which OID should be used for the imported large object.

The import process involves:
1. **Parameter extraction**: Gets both the filename and desired OID from the function arguments
2. **Delegation**: Calls the internal import function with the user-specified OID
3. **Return OID**: Returns the OID of the newly created large object (should match the input OID if successful)

This function provides more control over large object creation by allowing explicit OID assignment, which can be useful for data migration, replication, or when specific OID values are required.

## Parameters / Member Variables
-  (text*): The filesystem path of the file to import, obtained from 
-  (Oid): The specific OID to assign to the imported large object, obtained from 

## Dependencies
- Functions called/Symbols referenced:
  - [lo_import_internal](../l/lo_import_internal.md)
  - PG_GETARG_TEXT_PP (macro)
  - PG_GETARG_OID (macro)
  - PG_RETURN_OID (macro)
- Called from (representative examples):
  - No direct references found (likely called through function manager)

## Notes and Other Information
- This function differs from  by accepting a user-specified OID rather than auto-generating one
- The specified OID must be available (not already in use) or the import will fail
- The actual file reading and large object creation logic is implemented in 
- File access permissions, existence checking, and OID validation are handled by the internal import function
- Located in 
- Part of the Import/Export section of large object functionality
- Useful for scenarios requiring deterministic OID assignment, such as backup restoration or data synchronization