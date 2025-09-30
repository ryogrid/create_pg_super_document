# checkSharedDependencies

## Location
[src/backend/catalog/pg_shdepend.c:676-715](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L676-L715)

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
  - [IsPinnedObject](../I/IsPinnedObject.md) (checks if object is required by database system)
  - [getObjectDescription](../g/getObjectDescription.md) (formats object descriptions)
  - [SysScanDesc](../S/SysScanDesc.md) (system catalog scanning)
  - ShDependObjectInfo (dependency object information structure)
  - [shared_dependency_comparator](../s/shared_dependency_comparator.md) (for sorting dependency results)
  - [storeObjectDescription](../s/storeObjectDescription.md) (for formatting dependency descriptions)
- Called from (representative examples):
  - [DropTableSpace](../D/DropTableSpace.md) (tablespace deletion in tablespace.c:453)
  - [DropRole](../D/DropRole.md) (role deletion in user.c:1295)

## Notes and Other Information
- Returns false if no dependencies exist, true if dependencies are found
- Limits client-reported dependencies to MAX_REPORTED_DEPS (100) for manageable error messages
- Always provides complete dependency list in server log regardless of size
- Sorts local and shared objects by OID for stable regression test results  
- Handles remote database dependencies by providing counts rather than detailed descriptions
- Immediately errors for pinned objects that are required by the database system
- Uses dynamic memory allocation for dependency arrays, starting with 128 objects and doubling as needed
- Distinguishes between LOCAL_OBJECT, SHARED_OBJECT, and REMOTE_OBJECT dependency types

## Simplified Source

```c
bool checkSharedDependencies(Oid classId, Oid objectId,
                           char **detail_msg, char **detail_log_msg) {
    Relation sdepRel;
    SysScanDesc scan;
    HeapTuple tup;
    int numReportedDeps = 0;
    int numNotReportedDeps = 0;
    ObjectAddress object;
    ShDependObjectInfo *objects;
    StringInfoData descs, alldescs;

    // Quick check: if object is pinned (required by system), error immediately
    if (IsPinnedObject(classId, objectId)) {
        object.classId = classId;
        object.objectId = objectId;
        object.objectSubId = 0;
        ereport(ERROR, (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                       errmsg("cannot drop %s because it is required by the database system",
                              getObjectDescription(&object, false))));
    }

    // Step 1: Scan pg_shdepend catalog for dependencies
    sdepRel = table_open(SharedDependRelationId, AccessShareLock);

    ScanKeyInit(&key[0], Anum_pg_shdepend_refclassid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classId));
    ScanKeyInit(&key[1], Anum_pg_shdepend_refobjid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objectId));

    scan = systable_beginscan(sdepRel, SharedDependReferenceIndexId, true, NULL, 2, key);

    // Step 2: Collect and categorize dependencies
    initStringInfo(&descs);
    initStringInfo(&alldescs);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_shdepend shdepForm = (Form_pg_shdepend) GETSTRUCT(tup);

        // Categorize dependency: LOCAL_OBJECT, SHARED_OBJECT, or REMOTE_OBJECT
        if (shdepForm->dbid == MyDatabaseId) {
            // Local object dependency - get description
            collect_local_dependency(shdepForm, &objects, &numReportedDeps);
        } else if (shdepForm->dbid == InvalidOid) {
            // Shared object dependency - get description
            collect_shared_dependency(shdepForm, &objects, &numReportedDeps);
        } else {
            // Remote database dependency - just count
            numNotReportedDeps++;
        }
    }

    systable_endscan(scan);
    table_close(sdepRel, AccessShareLock);

    // Step 3: Format dependency descriptions
    if (numReportedDeps > 0) {
        // Sort objects for consistent output
        qsort(objects, numReportedDeps, sizeof(ShDependObjectInfo), shared_dependency_comparator);

        // Build description strings (limited for client, complete for log)
        for (int i = 0; i < numReportedDeps; i++) {
            char *objdesc = getObjectDescription(&objects[i].object, false);

            if (i < MAX_REPORTED_DEPS) {
                appendStringInfo(&descs, "%s\n", objdesc);
            }
            appendStringInfo(&alldescs, "%s\n", objdesc);
        }
    }

    // Step 4: Add remote dependency summary if any
    if (numNotReportedDeps > 0) {
        appendStringInfo(&alldescs, "%d dependencies from other databases\n", numNotReportedDeps);
    }

    // Step 5: Set output parameters and return result
    *detail_msg = (descs.len > 0) ? descs.data : NULL;
    *detail_log_msg = (alldescs.len > 0) ? alldescs.data : NULL;

    return (numReportedDeps > 0 || numNotReportedDeps > 0);
}
```