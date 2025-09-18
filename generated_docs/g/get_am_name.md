# get_am_name

## Location
src/backend/commands/amcmds.c: 192 - 211

## Overview
Retrieves the name of an access method given its object identifier (OID) from the PostgreSQL system catalog.

## Definition
```c
char *get_am_name(Oid amOid)
```

## Detailed Description
This function performs a system catalog lookup to find the access method name corresponding to a given OID. It searches the pg_am system catalog using the provided access method OID and returns a dynamically allocated copy of the access method's name. The function returns NULL if no access method with the specified OID exists.

The implementation uses PostgreSQL's system cache (syscache) for efficient lookup of access method information, specifically using the AMOID cache to find entries by OID.

## Parameters / Member Variables
- `amOid`: The object identifier (OID) of the access method whose name is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [pstrdup](../p/pstrdup.md)
  - NameStr
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_am

- Called from (representative examples):
  - [getObjectIdentityParts](getObjectIdentityParts.md) (src/backend/catalog/objectaddress.c:5099)
  - [assignOperTypes](../a/assignOperTypes.md) (src/backend/commands/opclasscmds.c:1174)
  - [IsThereOpClassInNamespace](../I/IsThereOpClassInNamespace.md) (src/backend/commands/opclasscmds.c:1817)
  - [IsThereOpFamilyInNamespace](../I/IsThereOpFamilyInNamespace.md) (src/backend/commands/opclasscmds.c:1840)

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller
- Returns NULL if the access method OID is not found in the system catalog
- Uses the system cache for efficient lookup performance
- Part of the access method command utilities in PostgreSQL
- The returned string is a copy of the name stored in the pg_am catalog