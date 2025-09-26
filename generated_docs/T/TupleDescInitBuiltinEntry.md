# TupleDescInitBuiltinEntry

## Location
[src/backend/access/common/tupdesc.c:726-832](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L726-L832)

## Overview
Initializes a tuple descriptor attribute entry without requiring catalog access by supporting only a limited range of builtin PostgreSQL data types.

## Definition

```c
void
TupleDescInitBuiltinEntry(TupleDesc desc,
						  AttrNumber attributeNumber,
						  const char *attributeName,
						  Oid oidtypeid,
						  int32 typmod,
						  int attdim)
```
## Detailed Description
TupleDescInitBuiltinEntry is designed to initialize tuple descriptor attributes for essential builtin types without needing access to the system catalog (pg_type table). This function is particularly useful in scenarios where database catalog access is not available, such as during bootstrap processes or in disconnected utility operations.

The function supports only a core set of PostgreSQL builtin types: TEXT, TEXTARRAY, BOOL, INT4, INT8, and OID. For each supported type, it sets the appropriate type-specific attributes including length, alignment, storage method, and collation settings.

Unlike TupleDescInitEntry which can handle any type by consulting the catalog, this function uses hardcoded type information for the supported builtin types, making it suitable for use in constrained environments.

## Parameters / Member Variables
- : The tuple descriptor to modify
- : The 1-based position of the attribute within the tuple descriptor
- : The name to assign to the attribute (required, cannot be NULL)
- : The OID of the PostgreSQL data type (must be one of the supported builtin types)
- : Type modifier value for the attribute
- : Number of array dimensions (0 for non-array types)

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid (validation macro)
  - [namestrcpy](../n/namestrcpy.md) (string copying utility)
  - TupleDescAttr (tuple descriptor accessor macro)
  - Various type alignment constants (TYPALIGN_INT, TYPALIGN_CHAR, TYPALIGN_DOUBLE)
  - Storage method constants (TYPSTORAGE_EXTENDED, TYPSTORAGE_PLAIN)
  - InvalidCompressionMethod constant
- Called from (representative examples):
  - [SendXlogRecPtrResult](../S/SendXlogRecPtrResult.md) (basebackup functionality)
  - [IdentifySystem](../I/IdentifySystem.md) (replication protocol)
  - [ShowGUCConfigOption](../S/ShowGUCConfigOption.md) (configuration display)
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md) (replication slot management)

## Notes and Other Information
- Only supports TEXT, TEXTARRAY, BOOL, INT4, INT8, and OID types; attempting to use other types will result in an ERROR
- Requires an attribute name (unlike TupleDescInitEntry which can accept NULL)
- Sets attislocal to true and attinhcount to 0, indicating the attribute is locally defined
- Designed for use in environments where catalog access is unavailable or undesirable
- All variable-length fields in the attribute structure are not set as they are not present in tupledescs
- Type-specific attributes like length, alignment, and storage are hardcoded based on the builtin type properties