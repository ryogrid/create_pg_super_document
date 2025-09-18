# be_lo_unlink

## Location
[src/backend/libpq/be-fsstubs.c:314-356](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-fsstubs.c#L314-L356)

## Overview
Deletes a large object from the database and returns the result of the deletion operation.

## Definition


## Detailed Description
This function implements the backend functionality for deleting large objects in PostgreSQL. It performs several important operations:

1. **Read-only protection**: Prevents deletion in read-only transactions
2. **Permission checking**: Ensures the current user owns the large object (unless compatibility mode is enabled)
3. **File descriptor cleanup**: Closes any open file descriptors referencing the large object
4. **Physical deletion**: Calls the low-level  function to remove the large object from storage

The function is designed to be called through the SQL  function and handles all the necessary safety checks and cleanup operations before performing the actual deletion.

## Parameters / Member Variables
-  (Oid): The OID of the large object to be deleted, obtained from 

## Dependencies
- Functions called/Symbols referenced:
  - [PreventCommandIfReadOnly](../P/PreventCommandIfReadOnly.md)
  - [object_ownercheck](../o/object_ownercheck.md)  
  - [closeLOfd](../c/closeLOfd.md)
  - [inv_drop](../i/inv_drop.md)
- Called from (representative examples):
  - No direct references found (likely called through function manager)

## Notes and Other Information
- The function checks ownership permissions unless  is enabled
- Any open file descriptors to the large object are automatically closed before deletion
- The function does not require end-of-transaction cleanup since  handles this internally
- Returns an integer result from  indicating success/failure of the deletion operation
- Located in 