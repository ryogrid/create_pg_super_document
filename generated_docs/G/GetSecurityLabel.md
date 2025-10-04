# GetSecurityLabel

## Location
[src/backend/commands/seclabel.c:272-328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/seclabel.c#L272-L328)

## Overview
Retrieves the security label for any database object (shared or unshared) from the appropriate system catalog for a specified provider.

## Definition
```c
char *GetSecurityLabel(const ObjectAddress *object, const char *provider)
```

## Detailed Description
This function serves as the main entry point for retrieving security labels from PostgreSQL's security label system. It acts as a dispatcher that determines whether the target object is a shared object (cluster-wide) or a database-specific object, then delegates to the appropriate retrieval mechanism.

For shared objects (identified using IsSharedRelation()), the function delegates to GetSharedSecurityLabel() which searches the pg_shseclabel catalog. For regular database objects, it performs a direct scan of the pg_seclabel catalog using a four-key search that includes the object OID, class OID, sub-object ID, and provider name.

The function uses the SecLabelObjectIndexId index for efficient lookups and handles both the absence of any matching tuple and the presence of a tuple with a NULL label value by returning NULL in both cases.

## Parameters / Member Variables
- `object`: A pointer to an ObjectAddress structure containing the objectId, classId, and objectSubId of the target object
- `provider`: A C string specifying the name of the security label provider for which to retrieve the label

## Dependencies
- Functions called/Symbols referenced:
  - [IsSharedRelation](../I/IsSharedRelation.md)
  - [GetSharedSecurityLabel](GetSharedSecurityLabel.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [table_open](../t/table_open.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [heap_getattr](../h/heap_getattr.md)
  - TextDatumGetCString
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [Int32GetDatum](../I/Int32GetDatum.md)
  - CStringGetTextDatum
  - RelationGetDescr
  - HeapTupleIsValid
- Called from (representative examples):
  - Functions requiring security label retrieval (referenced in seclabel.h)

## Notes and Other Information
- Returns a dynamically allocated C string containing the security label, or NULL if no label exists for the specified object and provider
- Automatically handles the distinction between shared and unshared objects, making it the unified interface for security label retrieval
- Uses a four-key scan for unshared objects (objectId, classId, objectSubId, provider) versus a three-key scan for shared objects (which don't have sub-object IDs)
- Applies AccessShareLock on the appropriate catalog (pg_seclabel or pg_shseclabel) to ensure consistent reads
- The returned string (if not NULL) should be freed by the caller when no longer needed
- This is the public interface function for security label retrieval, as indicated by its inclusion in the header file
- The objectSubId parameter allows for security labels on sub-objects like table columns, which is not applicable to shared objects

## Simplified Source

```c
char *GetSecurityLabel(const ObjectAddress *object, const char *provider) {
    // Shared objects have their own security label catalog
    if (IsSharedRelation(object->classId))
        return GetSharedSecurityLabel(object, provider);

    // Handle unshared objects using pg_seclabel catalog
    Relation pg_seclabel;
    ScanKeyData keys[4];
    SysScanDesc scan;
    HeapTuple tuple;
    char *seclabel = NULL;

    // Set up scan keys for object ID, class ID, sub-object ID, and provider
    ScanKeyInit(&keys[0], Anum_pg_seclabel_objoid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(object->objectId));
    ScanKeyInit(&keys[1], Anum_pg_seclabel_classoid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(object->classId));
    ScanKeyInit(&keys[2], Anum_pg_seclabel_objsubid, BTEqualStrategyNumber, F_INT4EQ,
                Int32GetDatum(object->objectSubId));
    ScanKeyInit(&keys[3], Anum_pg_seclabel_provider, BTEqualStrategyNumber, F_TEXTEQ,
                CStringGetTextDatum(provider));

    // Open catalog and perform indexed scan
    pg_seclabel = table_open(SecLabelRelationId, AccessShareLock);
    scan = systable_beginscan(pg_seclabel, SecLabelObjectIndexId, true, NULL, 4, keys);

    // Get matching tuple and extract label if found
    tuple = systable_getnext(scan);
    if (HeapTupleIsValid(tuple)) {
        Datum datum = heap_getattr(tuple, Anum_pg_seclabel_label,
                                   RelationGetDescr(pg_seclabel), &isnull);
        if (!isnull)
            seclabel = TextDatumGetCString(datum);
    }

    // Clean up and return result
    systable_endscan(scan);
    table_close(pg_seclabel, AccessShareLock);
    return seclabel;
}
```