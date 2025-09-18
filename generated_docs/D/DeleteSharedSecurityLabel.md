# DeleteSharedSecurityLabel

## Location
src/backend/commands/seclabel.c: 491 - 522

## Overview
DeleteSharedSecurityLabel removes all security labels associated with a specified shared database object.

## Definition


## Detailed Description
DeleteSharedSecurityLabel is a helper function of DeleteSecurityLabel specifically designed to handle shared database objects. Shared objects in PostgreSQL are those that exist at the cluster level rather than within individual databases, such as roles, tablespaces, and databases themselves. The function deletes all security label entries for the specified object from the pg_shseclabel system catalog.

The function performs the following operations:
1. Opens the pg_shseclabel system catalog with RowExclusiveLock
2. Sets up a scan using the SharedSecLabelObjectIndexId index to find all entries matching the object
3. Iterates through all matching tuples and deletes each one using CatalogTupleDelete
4. Closes the catalog and releases the lock

## Parameters / Member Variables
- : The OID of the shared database object whose security labels are to be deleted
- : The OID of the system catalog class that the object belongs to (e.g., AuthIdRelationId for roles)

## Dependencies
- Functions called/Symbols referenced:
  - systable_beginscan
  - systable_getnext
  - CatalogTupleDelete
- Called from (representative examples):
  - dropdb
  - DeleteSecurityLabel
  - DropTableSpace
  - DropRole

## Notes and Other Information
- This function is specifically for shared objects - regular database objects use DeleteSecurityLabel instead
- The function deletes ALL security labels for the object regardless of provider, making it suitable for cleanup during object deletion
- Uses a while loop to handle cases where an object might have multiple security labels from different providers
- The function is typically called during DROP operations for shared objects to ensure proper cleanup
- No return value since this is a cleanup operation that should always succeed or raise an error