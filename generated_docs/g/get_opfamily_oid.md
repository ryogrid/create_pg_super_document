# get_opfamily_oid

## Location
[src/backend/commands/opclasscmds.c:139-161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/opclasscmds.c#L139-L161)

## Overview
get_opfamily_oid is a utility function that finds an operator family OID by its possibly qualified name, serving as a convenient wrapper around OpFamilyCacheLookup.

## Definition
```c
Oid get_opfamily_oid(Oid amID, List *opfamilyname, bool missing_ok)
```

## Detailed Description
This function provides a clean interface for retrieving operator family OIDs from the system catalog. It leverages the OpFamilyCacheLookup function to perform the actual catalog lookup, then extracts the OID from the returned tuple structure. The function handles proper memory management by releasing the syscache tuple after extracting the required information.

The function is designed as a higher-level abstraction that simplifies the common task of converting operator family names to their corresponding OIDs, which are frequently needed throughout the PostgreSQL system for various operations involving operator families.

## Parameters
- `amID`: The OID of the access method that the operator family belongs to
- `opfamilyname`: A list representing the qualified or unqualified name of the operator family to look up
- `missing_ok`: If true, returns InvalidOid when the operator family is not found; if false, allows the underlying function to raise an error

## Dependencies
- Functions called/Symbols referenced:
  - [OpFamilyCacheLookup](../O/OpFamilyCacheLookup.md)
  - Form_pg_opfamily
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [get_object_address_opcf](get_object_address_opcf.md)
  - [DefineOpClass](../D/DefineOpClass.md)
  - [AlterOpFamily](../A/AlterOpFamily.md)
  - [AlterOpFamilyAdd](../A/AlterOpFamilyAdd.md)

## Notes and Other Information
- This function is exported and can be called from other source files, as evidenced by its declaration in defrem.h
- Proper syscache memory management is implemented by calling ReleaseSysCache() after extracting the OID
- The function returns InvalidOid rather than throwing an error when missing_ok is true, making it suitable for optional lookups
- Commonly used throughout the operator class and family management subsystem in PostgreSQL