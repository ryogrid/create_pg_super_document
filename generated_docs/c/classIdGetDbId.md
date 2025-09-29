# classIdGetDbId

## Location
[src/backend/catalog/pg_shdepend.c:1190-1210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L1190-L1210)

## Overview
Utility function that determines the appropriate database ID to use in pg_shdepend records based on whether the catalog table contains shared or database-specific objects.

## Definition

```c
static Oid
classIdGetDbId(Oid classId)
```
## Detailed Description
This function provides a standardized way to determine the correct database ID (dbid) field value for shared dependency records in pg_shdepend. The function implements the logic that shared catalog tables (those visible across all databases in a cluster) should use InvalidOid (0) as their database ID, while database-specific catalog tables should use the current database's OID.

This distinction is crucial for proper shared dependency tracking, as it allows the system to differentiate between dependencies that are cluster-wide (shared) versus those that are database-specific.

## Parameters / Member Variables
- : OID of the catalog table/relation containing the object

## Dependencies
- Functions called/Symbols referenced:
  - [IsSharedRelation](../I/IsSharedRelation.md)
  - MyDatabaseId (global variable)
  - InvalidOid (constant)
- Called from (representative examples):
  - [shdepAddDependency](../s/shdepAddDependency.md)
  - [shdepDropDependency](../s/shdepDropDependency.md)
  - [shdepChangeDep](../s/shdepChangeDep.md)
  - ShDependObjectInfo

## Notes and Other Information
- This is a static internal function, not directly accessible outside pg_shdepend.c
- The function is essential for maintaining the proper database context in shared dependency records
- Shared relations (like pg_authid, pg_tablespace) get InvalidOid as their database ID
- Database-specific relations get the current database's OID from MyDatabaseId
- The return value directly determines the dbid field in pg_shdepend tuples
- Simple but critical function for the shared dependency tracking system's correctness

## Simplified Source

```c
static Oid
classIdGetDbId(Oid classId)
{
    // Shared catalogs use InvalidOid (0), others use current database ID
    if (IsSharedRelation(classId))
        return InvalidOid;
    else
        return MyDatabaseId;
}
```