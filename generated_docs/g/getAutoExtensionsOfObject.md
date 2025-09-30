# getAutoExtensionsOfObject

## Location
[src/backend/catalog/pg_depend.c:779-828](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_depend.c#L779-L828)

## Overview
Returns a list of extensions that the specified object depends on through DEPENDENCY_AUTO_EXTENSION relationships.

## Definition

```c
List *
getAutoExtensionsOfObject(Oid classId, Oid objectId)
```
## Detailed Description
The `getAutoExtensionsOfObject` function searches the `pg_depend` system catalog to find all extensions that have an automatic extension dependency relationship with a given database object. Unlike regular extension membership (DEPENDENCY_EXTENSION), automatic extension dependencies (DEPENDENCY_AUTO_EXTENSION) represent a different kind of relationship where objects are automatically associated with extensions but can exist independently.

The function performs a comprehensive scan of the `pg_depend` table, collecting all dependency records where:
- The `classid` and `objid` match the specified object
- The `refclassid` points to ExtensionRelationId (indicating the dependency target is an extension)
- The `deptype` is DEPENDENCY_AUTO_EXTENSION (indicating automatic extension dependency)

For each matching dependency, the function adds the extension's OID (`refobjid`) to the result list. The function continues scanning through all dependency records to collect all applicable extensions, as objects may have automatic dependencies on multiple extensions.

## Parameters / Member Variables
- `classId`: The OID of the system catalog that contains the object (e.g., RelationRelationId for tables)
- `objectId`: The OID of the specific object within that catalog

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](../S/SysScanDesc.md)
  - [systable_beginscan](../s/systable_beginscan.md)  
  - [systable_getnext](../s/systable_getnext.md)
  - Form_pg_depend
  - DEPENDENCY_AUTO_EXTENSION
  - [lappend_oid](../l/lappend_oid.md)
- Called from (representative examples):
  - [ExecAlterObjectDependsStmt](../E/ExecAlterObjectDependsStmt.md)
  - PERFORM_DELETION_CONCURRENT_LOCK

## Notes and Other Information
- Returns NIL (empty list) if the object has no automatic extension dependencies
- Unlike `getExtensionOfObject`, this function can return multiple extensions since objects may have automatic dependencies on several extensions
- Uses the `lappend_oid` function to build the result list dynamically during the scan
- The DEPENDENCY_AUTO_EXTENSION dependency type represents a weaker form of extension association compared to regular extension membership
- This function is particularly important for the ALTER ... DEPENDS ON EXTENSION functionality
- Uses AccessShareLock when accessing the pg_depend catalog for consistent reads

## Simplified Source

```c
List *getAutoExtensionsOfObject(Oid classId, Oid objectId) {
    List *result = NIL;
    Relation depRel;
    ScanKeyData key[2];
    SysScanDesc scan;
    HeapTuple tup;

    // Step 1: Open pg_depend catalog for reading
    depRel = table_open(DependRelationId, AccessShareLock);

    // Step 2: Set up scan keys to find dependencies for the specified object
    ScanKeyInit(&key[0], Anum_pg_depend_classid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classId));
    ScanKeyInit(&key[1], Anum_pg_depend_objid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objectId));

    // Step 3: Begin indexed scan using DependDependerIndexId
    scan = systable_beginscan(depRel, DependDependerIndexId, true, NULL, 2, key);

    // Step 4: Scan through all dependency records for this object
    while (HeapTupleIsValid((tup = systable_getnext(scan)))) {
        Form_pg_depend depform = (Form_pg_depend) GETSTRUCT(tup);

        // Step 5: Check if this is an automatic extension dependency
        if (depform->refclassid == ExtensionRelationId &&
            depform->deptype == DEPENDENCY_AUTO_EXTENSION) {
            // Add extension OID to result list
            result = lappend_oid(result, depform->refobjid);
        }
    }

    // Step 6: Clean up scan and close catalog
    systable_endscan(scan);
    table_close(depRel, AccessShareLock);

    return result;
}
```