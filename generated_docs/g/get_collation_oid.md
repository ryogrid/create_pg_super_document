# get_collation_oid

## Location
[src/backend/catalog/namespace.c:3971-4024](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L3971-L4024)

## Overview
Finds a collation by its possibly qualified name and returns its OID, ensuring the collation is compatible with the current database's encoding.

## Definition


## Detailed Description
This function searches for a collation by name, which can be either a simple name or a schema-qualified name. It ensures that only collations compatible with the current database's encoding are considered. If a schema is explicitly provided, the function searches only in that schema. Otherwise, it searches through the active search path, skipping the temporary namespace. The function can either raise an error or return InvalidOid when the collation is not found, depending on the missing_ok parameter.

## Parameters / Member Variables
- : A List containing the collation name, possibly schema-qualified
- : If true, returns InvalidOid when collation not found; if false, raises an error

## Dependencies
- Functions called/Symbols referenced:
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - [DeconstructQualifiedName](../D/DeconstructQualifiedName.md)
  - [LookupExplicitNamespace](../L/LookupExplicitNamespace.md)
  - [lookup_collation](../l/lookup_collation.md)
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [NameListToString](../N/NameListToString.md)
  - [GetDatabaseEncodingName](../G/GetDatabaseEncodingName.md)
- Called from (representative examples):
  - [get_object_address](get_object_address.md)
  - [DefineCollation](../D/DefineCollation.md)
  - [AlterCollation](../A/AlterCollation.md)
  - [ComputeIndexAttrs](../C/ComputeIndexAttrs.md)
  - [DefineDomain](../D/DefineDomain.md)
  - [LookupCollation](../L/LookupCollation.md)

## Notes and Other Information
- Only finds collations that work with the current database's encoding
- Skips the temporary namespace when searching through the search path
- Returns InvalidOid if collation not found and missing_ok is true
- Raises ERRCODE_UNDEFINED_OBJECT error if collation not found and missing_ok is false
- The error message includes both the collation name and the database encoding name for clarity