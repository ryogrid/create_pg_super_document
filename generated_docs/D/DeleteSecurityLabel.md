# DeleteSecurityLabel

## Location
src/backend/commands/seclabel.c: 523 - 569

## Overview
DeleteSecurityLabel removes all security labels for a specified database object and any sub-objects if applicable.

## Definition


## Detailed Description
DeleteSecurityLabel removes all security labels associated with a database object from the appropriate system catalog. The function handles both regular objects (using pg_seclabel) and shared objects (delegating to DeleteSharedSecurityLabel for pg_shseclabel). For regular objects, it can delete labels for either a specific sub-object (when objectSubId is non-zero) or all sub-objects of the main object (when objectSubId is zero).

The function performs the following operations:
1. Checks if the object is a shared relation and delegates to DeleteSharedSecurityLabel if so
2. Sets up scan keys to locate security label entries for the target object
3. Uses either 2 or 3 scan keys depending on whether a specific sub-object is targeted
4. Scans through all matching tuples and deletes each one
5. Properly handles cleanup and lock management

## Parameters / Member Variables
- : Pointer to ObjectAddress structure identifying the target database object (contains classId, objectId, and objectSubId)

## Dependencies
- Functions called/Symbols referenced:
  - IsSharedRelation
  - DeleteSharedSecurityLabel
  - systable_beginscan
  - systable_getnext
  - CatalogTupleDelete
- Called from (representative examples):
  - deleteOneObject

## Notes and Other Information
- The function handles both specific sub-object deletion (objectSubId != 0) and wholesale object deletion (objectSubId == 0)
- For shared objects, it asserts that objectSubId must be 0, as shared objects don't have sub-objects
- Uses different numbers of scan keys (2 or 3) depending on whether targeting a specific sub-object
- This function is typically called during object deletion as part of the dependency cleanup process
- Deletes ALL security labels for the object regardless of provider, making it suitable for complete cleanup
- The function is part of PostgreSQL's object deletion cascade system managed by the dependency subsystem