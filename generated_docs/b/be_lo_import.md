# be_lo_import

## Location
[src/backend/libpq/be-fsstubs.c:398-409](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-fsstubs.c#L398-L409)

## Overview
Imports a file from the filesystem into the database as a large object and returns the OID of the newly created large object.

## Definition


## Detailed Description
This function implements the backend functionality for the SQL  function, which allows importing external files into PostgreSQL as large objects. The function serves as a thin wrapper around the internal  function.

The import process involves:
1. **Parameter extraction**: Gets the filename from the function arguments as a text value
2. **Delegation**: Calls the internal import function with , allowing the system to automatically assign a new OID
3. **Return OID**: Returns the OID of the newly created large object

This function provides the PostgreSQL function interface for file import operations, handling the conversion between SQL data types and internal representations.

## Parameters / Member Variables
-  (text*): The filesystem path of the file to import, obtained from 

## Dependencies
- Functions called/Symbols referenced:
  - [lo_import_internal](../l/lo_import_internal.md)
  - PG_GETARG_TEXT_PP (macro)
  - PG_RETURN_OID (macro)
  - InvalidOid (constant)
- Called from (representative examples):
  - No direct references found (likely called through function manager)

## Notes and Other Information
- This function uses  as the second parameter to , which causes the system to automatically assign a new OID to the imported large object
- The function is part of the Import/Export section of large object functionality
- File access permissions and existence are handled by the internal import function
- The actual file reading and large object creation logic is implemented in 
- Located in 
- For importing with a specific OID, see the related  function