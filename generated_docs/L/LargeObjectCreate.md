# LargeObjectCreate

## Location
[src/backend/catalog/pg_largeobject.c:37-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_largeobject.c#L37-L82)

## Overview
Creates a new large object in PostgreSQL by inserting metadata into the pg_largeobject_metadata catalog table, initially with size 0.

## Definition
```c
Oid LargeObjectCreate(Oid loid)
```

## Detailed Description
The LargeObjectCreate function creates a new large object by inserting an entry into the pg_largeobject_metadata catalog table without any actual data pages. This means the large object initially appears to exist with size 0. The function handles both cases where a specific OID is requested (if valid) or generates a new unique OID automatically. The function sets up the metadata including the owner (current user) and leaves the access control list (ACL) as null initially.

The creation process involves:
1. Opening the pg_largeobject_metadata relation with RowExclusiveLock
2. Setting up values array with the OID, owner, and null ACL
3. Creating and inserting a new heap tuple into the catalog
4. Cleaning up and closing the relation

## Parameters / Member Variables
- `loid`: The desired OID for the large object. If OidIsValid(loid) returns false, a new unique OID will be automatically generated.

## Dependencies
- Functions called/Symbols referenced:
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md) (generates new unique OID when needed)
  - [heap_form_tuple](../h/heap_form_tuple.md) (creates heap tuple from values/nulls arrays)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md) (inserts tuple into catalog table)
  - [heap_freetuple](../h/heap_freetuple.md) (frees memory allocated for heap tuple)
- Called from (representative examples):
  - [inv_create](../i/inv_create.md) (from src/backend/storage/large_object/inv_api.c:218)

## Notes and Other Information
- The function returns the OID of the newly created large object
- Initially creates large objects with size 0 - actual data is added through separate operations
- Sets the owner to the current user (GetUserId())
- The ACL (access control list) is initially set to null
- Uses RowExclusiveLock to ensure exclusive access during creation
- Located in src/backend/catalog/pg_largeobject.c:37-82