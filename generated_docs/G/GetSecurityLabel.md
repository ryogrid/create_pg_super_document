# GetSecurityLabel

## Location
src/backend/commands/seclabel.c: 272 - 328

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
  - IsSharedRelation
  - GetSharedSecurityLabel
  - ScanKeyInit
  - table_open
  - systable_beginscan
  - systable_getnext
  - heap_getattr
  - TextDatumGetCString
  - systable_endscan
  - table_close
  - ObjectIdGetDatum
  - Int32GetDatum
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