# checkSharedDependencies

## Location
src/backend/catalog/pg_shdepend.c: 676 - 715

## Overview
Checks for shared dependency entries for a given shared object and returns detailed information about dependent objects, used primarily during DROP operations to prevent deletion of objects that are still in use.

## Definition
```c
bool checkSharedDependencies(Oid classId, Oid objectId, char **detail_msg, char **detail_log_msg)
```

## Detailed Description
This function performs a comprehensive check for dependencies on a shared database object (such as roles, tablespaces, etc.) by scanning the pg_shdepend system catalog. It identifies three types of dependencies: objects in the current database, shared objects, and objects in remote databases. The function returns a boolean indicating whether dependencies exist and provides two detailed reports - one suitable for client error messages (limited in size) and another for complete server logging. The function handles pinned objects (required by the database system) by immediately throwing an error if a drop is attempted.

## Parameters / Member Variables
- `classId`: OID of the system catalog containing the object to check
- `objectId`: OID of the specific object being checked for dependencies  
- `detail_msg`: Output parameter for client-facing dependency description (size-limited)
- `detail_log_msg`: Output parameter for complete server log dependency description

## Dependencies
- Functions called/Symbols referenced:
  - IsPinnedObject (checks if object is required by database system)
  - getObjectDescription (formats object descriptions)
  - SysScanDesc (system catalog scanning)
  - ShDependObjectInfo (dependency object information structure)
  - shared_dependency_comparator (for sorting dependency results)
  - storeObjectDescription (for formatting dependency descriptions)
- Called from (representative examples):
  - DropTableSpace (tablespace deletion in tablespace.c:453)
  - DropRole (role deletion in user.c:1295)

## Notes and Other Information
- Returns false if no dependencies exist, true if dependencies are found
- Limits client-reported dependencies to MAX_REPORTED_DEPS (100) for manageable error messages
- Always provides complete dependency list in server log regardless of size
- Sorts local and shared objects by OID for stable regression test results  
- Handles remote database dependencies by providing counts rather than detailed descriptions
- Immediately errors for pinned objects that are required by the database system
- Uses dynamic memory allocation for dependency arrays, starting with 128 objects and doubling as needed
- Distinguishes between LOCAL_OBJECT, SHARED_OBJECT, and REMOTE_OBJECT dependency types