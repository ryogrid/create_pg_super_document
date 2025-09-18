# LargeObjectExists

## Location
src/backend/catalog/pg_largeobject.c: 155 - 184

## Overview
Checks whether a large object with the specified OID exists by scanning the pg_largeobject_metadata catalog table using a current snapshot.

## Definition
```c
bool LargeObjectExists(Oid loid)
```

## Detailed Description
The LargeObjectExists function determines if a large object exists by performing a system catalog scan on pg_largeobject_metadata. The function is designed with specific snapshot behavior considerations:

- It always uses an up-to-date snapshot when scanning the system catalog
- It does not use the system cache for large object metadata to avoid excessive local memory usage
- Should not be used when a large object is opened in read-only mode, as read-only mode operations should be relative to the caller's snapshot, while this function uses a current snapshot

The function performs a simple indexed lookup using the LargeObjectMetadataOidIndexId for efficient searching and returns true if a matching tuple is found, false otherwise. It uses AccessShareLock for the scan operation since it only needs to read the catalog.

## Parameters / Member Variables
- `loid`: The OID of the large object to check for existence. Must be a valid OID value.

## Dependencies
- Functions called/Symbols referenced:
  - SysScanDesc (system scan descriptor type)
  - systable_beginscan (begins system table scan)
  - systable_getnext (gets next tuple from system scan)
- Called from (representative examples):
  - objectNamesToOids (from src/backend/catalog/aclchk.c:735)
  - get_object_address (from src/backend/catalog/objectaddress.c:1049)
  - getObjectDescription (from src/backend/catalog/objectaddress.c:3134)
  - getObjectIdentityParts (from src/backend/catalog/objectaddress.c:5026)

## Notes and Other Information
- Returns boolean value: true if large object exists, false otherwise
- Uses AccessShareLock for non-blocking read access to the catalog
- Avoids system cache to prevent memory overhead
- Should not be used in read-only large object access contexts due to snapshot semantics
- Uses indexed scan for efficient lookup performance
- Part of the object address and ACL checking infrastructure
- Located in src/backend/catalog/pg_largeobject.c:155-184