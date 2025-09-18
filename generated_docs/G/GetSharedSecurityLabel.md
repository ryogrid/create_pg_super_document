# GetSharedSecurityLabel

## Location
src/backend/commands/seclabel.c: 224 - 271

## Overview
Retrieves the security label for a shared database object from the pg_shseclabel system catalog for a specified provider.

## Definition
```c
static char *GetSharedSecurityLabel(const ObjectAddress *object, const char *provider)
```

## Detailed Description
This function performs a catalog lookup to retrieve security labels for shared database objects (objects that exist cluster-wide rather than within a specific database). It searches the pg_shseclabel system catalog using a three-key scan to find the specific label for the given object and provider combination.

The function constructs a scan using the object's OID, class OID, and the provider name as search keys. It opens the pg_shseclabel catalog with an AccessShareLock for safe concurrent access, performs a system catalog scan using the SharedSecLabelObjectIndexId index for efficient lookup, and extracts the label text if found.

The function handles the case where no label exists by returning NULL, and properly manages catalog resources by closing the relation after the scan is complete.

## Parameters / Member Variables
- `object`: A pointer to an ObjectAddress structure containing the objectId and classId of the target shared object
- `provider`: A C string specifying the name of the security label provider for which to retrieve the label

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - table_open
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [heap_getattr](../h/heap_getattr.md)
  - TextDatumGetCString
  - [systable_endscan](../s/systable_endscan.md)
  - table_close
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - CStringGetTextDatum
  - RelationGetDescr
  - HeapTupleIsValid
- Called from (representative examples):
  - [GetSecurityLabel](GetSecurityLabel.md)

## Notes and Other Information
- Returns a dynamically allocated C string containing the security label, or NULL if no label exists for the specified object and provider
- Uses the SharedSecLabelObjectIndexId index for efficient catalog scanning
- Applies AccessShareLock on the pg_shseclabel catalog to ensure consistent reads while allowing concurrent access
- The criticalSharedRelcachesBuilt parameter in systable_beginscan indicates this function can be called during bootstrap when shared catalogs are being built
- Handles both the case where no tuple is found and where a tuple exists but the label field is NULL
- The returned string (if not NULL) should be freed by the caller when no longer needed
- This function specifically handles shared objects; for regular database objects, a different mechanism is used