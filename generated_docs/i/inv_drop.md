# inv_drop

## Location
src/backend/storage/large_object/inv_api.c: 349 - 377

## Overview
Destroys an existing large object permanently, removing it and all associated metadata from the database.

## Definition


## Detailed Description
The  function permanently removes a large object from the PostgreSQL database. It performs a cascading deletion that removes not only the large object data itself but also any associated comments and dependencies. The function uses the standard PostgreSQL dependency system to ensure referential integrity is maintained during deletion.

The function operates at the database level, removing the actual large object from storage rather than just closing a descriptor. After the deletion, it increments the command counter to ensure that the removal is visible to subsequent operations within the same transaction. This is important for transaction isolation and consistency.

## Parameters / Member Variables
- : The OID of the large object to be permanently deleted

## Dependencies
- Functions called/Symbols referenced:
  - [performDeletion](../p/performDeletion.md) (performs the actual deletion with dependency handling)
  - CommandCounterIncrement (advances command counter for transaction visibility)
  - DROP_CASCADE (deletion mode constant)
  - LargeObjectRelationId (relation identifier for large objects)
- Called from (representative examples):
  - [be_lo_unlink](../b/be_lo_unlink.md)

## Notes and Other Information
- The caller is expected to have already performed any required permission checks before calling this function
- Uses DROP_CASCADE mode to ensure all dependent objects are also removed
- The function always returns 1 on success for historical compatibility reasons
- After calling this function, the large object is permanently gone and cannot be recovered
- The command counter increment ensures the deletion is visible to subsequent operations in the same transaction
- This function affects the actual database storage, unlike inv_close which only affects local descriptors