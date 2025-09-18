# shdepDropDependency

## Location
[src/backend/catalog/pg_shdepend.c:1124-1189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L1124-L1189)

## Overview
Internal workhorse function for deleting shared dependency entries from the pg_shdepend catalog table, with flexible filtering options to target specific dependency relationships.

## Definition


## Detailed Description
This function provides flexible deletion of shared dependency records from the pg_shdepend catalog table. It performs systematic scanning and filtering to identify and remove dependency entries that match specified criteria. The function supports various filtering modes including dropping all subobject dependencies and filtering by reference object properties.

The function constructs scan keys to efficiently locate dependency records for the specified dependent object, then applies additional filtering based on the optional parameters before deleting matching entries. This ensures precise control over which dependency relationships are removed.

## Parameters / Member Variables
- : Open pg_shdepend relation with appropriate locks held by caller
- : OID of the catalog table containing the dependent object
- : OID of the dependent object itself
- : Sub-object identifier for the dependent object (ignored if drop_subobjects is true)
- : If true, ignore objsubId and consider all entries matching classId/objectId
- : OID of referenced object's catalog table (InvalidOid to ignore this filter)
- : OID of the referenced object (InvalidOid to ignore this filter)
- : Type of dependency to match (SHARED_DEPENDENCY_INVALID to ignore this filter)

## Dependencies
- Functions called/Symbols referenced:
  - [classIdGetDbId](../c/classIdGetDbId.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [Int32GetDatum](../I/Int32GetDatum.md)
  - [systable_beginscan](systable_beginscan.md)
  - [systable_getnext](systable_getnext.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - OidIsValid
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [systable_endscan](systable_endscan.md)
- Called from (representative examples):
  - [changeDependencyOnOwner](../c/changeDependencyOnOwner.md)
  - [changeDependencyOnTablespace](../c/changeDependencyOnTablespace.md)
  - [updateAclDependenciesWorker](../u/updateAclDependenciesWorker.md)
  - [dropDatabaseDependencies](../d/dropDatabaseDependencies.md)
  - [deleteSharedDependencyRecordsFor](../d/deleteSharedDependencyRecordsFor.md)

## Notes and Other Information
- This is a static internal function, not directly accessible outside pg_shdepend.c
- The function uses systematic table scanning with indexed access via SharedDependDependerIndexId
- Multiple filtering parameters allow for precise control over which dependencies are removed
- The drop_subobjects flag enables bulk removal of all subobject dependencies for an object
- Efficient scanning strategy using btree index on (dbid, classid, objid, objsubid)
- Assumes caller has proper locking on the pg_shdepend relation